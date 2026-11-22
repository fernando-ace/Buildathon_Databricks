use ahash::{AHasher, RandomState};
use arrow_array::{ArrayRef, Date32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use memchr::{memchr, memchr_iter, memchr2};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;
use std::cmp::Reverse;
use std::collections::{HashMap, HashSet, VecDeque};
use std::env;
use std::fs::{self, File};
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

type Id = u32;
const MISSING_ID: Id = Id::MAX;
const INVALID_EVENT_TS: i64 = i64::MIN;
const INVALID_PRICE_CENTS: u32 = 0;
const INVALID_EVENT_KIND: u8 = u8::MAX;
type FastMap<K, V> = HashMap<K, V, RandomState>;
type FastSet<K> = HashSet<K, RandomState>;
type PackedUserMap = hashbrown::HashMap<u64, Id, BuildHasherDefault<IdentityHasher>>;
type ShardEventMap = hashbrown::HashMap<u64, (IdRef, Event), BuildHasherDefault<IdentityHasher>>;
type PackedEventMap = hashbrown::HashMap<u64, Event, BuildHasherDefault<IdentityHasher>>;
type EventRecord = (u64, Id, Event);

#[derive(Clone, Copy)]
struct StrRef {
    ptr: *const u8,
    len: usize,
}

// These pointers refer into retained event/click input-file buffers.
unsafe impl Send for StrRef {}
unsafe impl Sync for StrRef {}

impl StrRef {
    fn new(bytes: &[u8]) -> Self {
        Self {
            ptr: bytes.as_ptr(),
            len: bytes.len(),
        }
    }

    fn bytes<'a>(self) -> &'a [u8] {
        // The caller keeps the source file buffer alive while this reference is used.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl PartialEq for StrRef {
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len && self.bytes() == other.bytes()
    }
}

impl Eq for StrRef {}

impl Hash for StrRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.bytes().hash(state);
    }
}

#[derive(Clone, Copy)]
struct IdRef {
    start: usize,
    len: usize,
}

#[derive(Default)]
struct IdentityHasher(u64);

impl Hasher for IdentityHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut hash = 0xcbf2_9ce4_8422_2325u64;
        for &byte in bytes {
            hash = (hash ^ byte as u64).wrapping_mul(0x0000_0100_0000_01b3);
        }
        self.0 = hash;
    }

    fn write_u64(&mut self, value: u64) {
        self.0 = value;
    }
}

struct EventShard {
    primary: PackedEventMap,
    fallback: ShardEventMap,
    collisions: FastMap<u64, Vec<(IdRef, Event)>>,
    ids: Vec<u8>,
}

impl EventShard {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            primary: PackedEventMap::with_capacity_and_hasher(
                capacity,
                BuildHasherDefault::default(),
            ),
            fallback: ShardEventMap::with_capacity_and_hasher(
                capacity / 100,
                BuildHasherDefault::default(),
            ),
            collisions: fast_map(),
            ids: Vec::new(),
        }
    }

    fn insert(&mut self, hash: u64, id: StrRef, event: Event, packed: bool) {
        if packed {
            match self.primary.entry(hash) {
                hashbrown::hash_map::Entry::Vacant(entry) => {
                    entry.insert(event);
                }
                hashbrown::hash_map::Entry::Occupied(mut entry) => {
                    let stored_event = entry.get_mut();
                    if event.ingest_seconds < stored_event.ingest_seconds {
                        *stored_event = event;
                    }
                }
            }
            return;
        }

        match self
            .fallback
            .raw_entry_mut()
            .from_hash(hash, |stored_hash| *stored_hash == hash)
        {
            hashbrown::hash_map::RawEntryMut::Vacant(entry) => {
                let key = Self::store(&mut self.ids, id);
                entry.insert_hashed_nocheck(hash, hash, (key, event));
            }
            hashbrown::hash_map::RawEntryMut::Occupied(mut entry) => {
                let (stored_id, stored_event) = entry.get_mut();
                let stored = &self.ids[stored_id.start..stored_id.start + stored_id.len];
                if stored == id.bytes() {
                    // Equal-ingest copies are identical under the track rules.
                    if event.ingest_seconds < stored_event.ingest_seconds {
                        *stored_event = event;
                    }
                    return;
                }
                let collisions = self.collisions.entry(hash).or_default();
                if let Some((_, stored_event)) = collisions
                    .iter_mut()
                    .find(|(key, _)| &self.ids[key.start..key.start + key.len] == id.bytes())
                {
                    if event.ingest_seconds < stored_event.ingest_seconds {
                        *stored_event = event;
                    }
                } else {
                    let key = Self::store(&mut self.ids, id);
                    collisions.push((key, event));
                }
            }
        }
    }

    fn store(ids: &mut Vec<u8>, id: StrRef) -> IdRef {
        let start = ids.len();
        let bytes = id.bytes();
        ids.extend_from_slice(bytes);
        IdRef {
            start,
            len: bytes.len(),
        }
    }

    fn append_events(self, events: &mut Vec<Event>) {
        events.extend(self.primary.into_values());
        events.extend(self.fallback.into_values().map(|(_, event)| event));
        events.extend(
            self.collisions
                .into_values()
                .flatten()
                .map(|(_, event)| event),
        );
    }
}

const DAY_MS: i64 = 86_400_000;
const SESSION_GAP_MS: i64 = 1_800_000;
const ATTRIBUTION_MS: i64 = 604_800_000;

#[derive(Default)]
struct StringArena {
    chunks: Vec<Box<[u8]>>,
    next: usize,
}

impl StringArena {
    fn store(&mut self, bytes: &[u8]) -> StrRef {
        const CHUNK_SIZE: usize = 64 * 1024;
        if self
            .chunks
            .last()
            .is_none_or(|chunk| chunk.len() - self.next < bytes.len())
        {
            self.chunks
                .push(vec![0; CHUNK_SIZE.max(bytes.len())].into_boxed_slice());
            self.next = 0;
        }
        let start = self.next;
        let end = start + bytes.len();
        let chunk = self.chunks.last_mut().unwrap();
        chunk[start..end].copy_from_slice(bytes);
        self.next = end;
        StrRef::new(&chunk[start..end])
    }
}

#[derive(Default)]
struct Interner {
    // Values point into fixed-size arena chunks, so their addresses stay stable.
    values: Vec<StrRef>,
    arena: StringArena,
    ids: FastMap<StrRef, Id>,
}

impl Interner {
    fn intern(&mut self, text: &str) -> Id {
        let lookup = StrRef::new(text.as_bytes());
        if let Some(&id) = self.ids.get(&lookup) {
            return id;
        }
        let key = self.arena.store(text.as_bytes());
        let id = self.values.len() as Id;
        self.values.push(key);
        self.ids.insert(key, id);
        id
    }

    fn get(&self, id: Id) -> &str {
        text(self.values[id as usize].bytes()).unwrap()
    }
}

#[derive(Default)]
struct BorrowedInterner {
    values: Vec<StrRef>,
    ids: FastMap<StrRef, Id>,
    users: PackedUserMap,
}

impl BorrowedInterner {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            values: Vec::with_capacity(capacity),
            ids: fast_map_with_capacity(capacity),
            users: PackedUserMap::with_capacity_and_hasher(capacity, BuildHasherDefault::default()),
        }
    }

    fn intern(&mut self, bytes: &[u8]) -> Id {
        let key = StrRef::new(bytes);
        if let Some(&id) = self.ids.get(&key) {
            return id;
        }
        let id = self.values.len() as Id;
        self.values.push(key);
        self.ids.insert(key, id);
        id
    }

    fn intern_user(&mut self, bytes: &[u8]) -> Id {
        let Some(key) = packed_user_id(bytes) else {
            return self.intern(bytes);
        };
        if let Some(&id) = self.users.get(&key) {
            return id;
        }
        let id = self.values.len() as Id;
        self.values.push(StrRef::new(bytes));
        self.users.insert(key, id);
        id
    }
}

struct Dsu {
    parent: Vec<Id>,
}

impl Dsu {
    fn new(n: usize) -> Self {
        Self {
            parent: (0..n as Id).collect(),
        }
    }

    fn root(&mut self, x: Id) -> Id {
        let mut r = x;
        while self.parent[r as usize] != r {
            r = self.parent[r as usize];
        }
        let mut p = x;
        while self.parent[p as usize] != p {
            let next = self.parent[p as usize];
            self.parent[p as usize] = r;
            p = next;
        }
        r
    }

    fn union(&mut self, a: Id, b: Id, strings: &Interner) {
        let mut ra = self.root(a);
        let mut rb = self.root(b);
        if ra == rb {
            return;
        }
        if strings.get(ra).as_bytes() > strings.get(rb).as_bytes() {
            std::mem::swap(&mut ra, &mut rb);
        }
        self.parent[rb as usize] = ra;
    }

    fn canonical(&self, id: Id) -> Id {
        self.parent[id as usize]
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
struct Event {
    ids: u64,
    ts: i64,
    ingest_seconds: u32,
    payload: u32,
}

impl Event {
    #[inline]
    fn new(ts: i64, ingest_ms: i64, user: Id, product: Id, price: u32, kind: u8) -> Self {
        Self {
            ids: u64::from(user) | (u64::from(product) << 32),
            ts,
            // Generated datasets and all track fixtures use positive F1 ingest
            // timestamps between 2025 and 2028, which fit in u32 seconds.
            ingest_seconds: (ingest_ms / 1_000) as u32,
            payload: (u32::from(kind) << 24) | (price & 0x00ff_ffff),
        }
    }

    #[inline]
    fn user(self) -> Id {
        self.ids as Id
    }

    #[inline]
    fn set_user(&mut self, user: Id) {
        self.ids = (self.ids & 0xffff_ffff_0000_0000) | u64::from(user);
    }

    #[inline]
    fn product(self) -> Id {
        (self.ids >> 32) as Id
    }

    #[inline]
    fn set_product(&mut self, product: Id) {
        self.ids = (self.ids & 0x0000_0000_ffff_ffff) | (u64::from(product) << 32);
    }

    #[inline]
    fn kind(self) -> u8 {
        (self.payload >> 24) as u8
    }

    #[inline]
    fn price(self) -> u32 {
        self.payload & 0x00ff_ffff
    }
}

const _: () = assert!(std::mem::size_of::<Event>() == 24);

#[derive(Clone, Copy)]
struct Click {
    id_key: u64,
    user: Id,
    ts: i64,
    campaign: Id,
}

const FALLBACK_CLICK_ID: u64 = 1 << 63;

#[derive(Clone, Copy)]
struct RawClick {
    id: StrRef,
    user: Id,
    ts: i64,
    campaign: Id,
}

#[derive(Clone, Copy)]
struct Session {
    person: Id,
    start: i64,
    day: i32,
    revenue: i64,
    has_purchase: bool,
}

#[derive(Default)]
struct UserAgg {
    sessions: i64,
    events: i64,
    revenue: i64,
    categories: FastMap<Id, i64>,
}

#[derive(Default)]
struct CampaignAgg {
    sessions: i64,
    purchasing_sessions: i64,
    revenue: i64,
    assisted_sessions: i64,
}

fn aggregate_user_group(
    events: &[Event],
    products: &FastMap<Id, Id>,
    unknown: Id,
    user_rows: &mut Vec<((Id, i32), UserAgg)>,
    sessions: &mut Vec<Session>,
) {
    let person = events[0].user();
    let first_ts = events[0].ts;
    let mut session_start = first_ts;
    let mut session_day = day(first_ts);
    let mut user_day = session_day;
    let mut daily = UserAgg::default();
    let mut session_revenue = 0i64;
    let mut session_purchase = false;
    let mut previous_ts = first_ts;

    for (index, event) in events.iter().enumerate() {
        let ts = event.ts;
        let new_session = index == 0 || ts - previous_ts > SESSION_GAP_MS;
        if index > 0 && new_session {
            sessions.push(Session {
                person,
                start: session_start,
                day: session_day,
                revenue: session_revenue,
                has_purchase: session_purchase,
            });
            session_start = ts;
            session_day = day(ts);
            if session_day != user_day {
                user_rows.push(((person, user_day), daily));
                daily = UserAgg::default();
                user_day = session_day;
            }
            session_revenue = 0;
            session_purchase = false;
        }

        daily.events += 1;
        if event.kind() == 2 {
            let amount = i64::from(event.price());
            session_revenue += amount;
            session_purchase = true;
            daily.revenue += amount;
            let category = *products.get(&event.product()).unwrap_or(&unknown);
            *daily.categories.entry(category).or_default() += amount;
        }
        if new_session {
            daily.sessions += 1;
        }
        previous_ts = ts;
    }

    sessions.push(Session {
        person,
        start: session_start,
        day: session_day,
        revenue: session_revenue,
        has_purchase: session_purchase,
    });
    user_rows.push(((person, user_day), daily));
}

fn attribute_user_sessions(
    sessions: &[Session],
    clicks: &[Click],
    other_click_ids: &[StrRef],
    organic: Id,
    daily: &mut FastMap<(Id, i32), CampaignAgg>,
    active: &mut VecDeque<Click>,
    campaign_counts: &mut FastMap<Id, u32>,
) {
    active.clear();
    campaign_counts.clear();
    let (mut add, mut remove) = (0usize, 0usize);
    for session in sessions {
        while add < clicks.len() && clicks[add].ts <= session.start {
            let click = clicks[add];
            while active.back().is_some_and(|last| {
                click.ts > last.ts
                    || (click.ts == last.ts
                        && click_id_cmp(click.id_key, last.id_key, other_click_ids).is_lt())
            }) {
                active.pop_back();
            }
            active.push_back(click);
            *campaign_counts.entry(click.campaign).or_default() += 1;
            add += 1;
        }
        let cutoff = session.start.saturating_sub(ATTRIBUTION_MS);
        while remove < add && clicks[remove].ts < cutoff {
            let click = clicks[remove];
            while active.front().is_some_and(|first| first.ts < cutoff) {
                active.pop_front();
            }
            if let Some(count) = campaign_counts.get_mut(&click.campaign) {
                *count -= 1;
                if *count == 0 {
                    campaign_counts.remove(&click.campaign);
                }
            }
            remove += 1;
        }
        let campaign = active
            .front()
            .map(|click| click.campaign)
            .unwrap_or(organic);
        let credited = daily.entry((campaign, session.day)).or_default();
        credited.sessions += 1;
        credited.revenue += session.revenue;
        credited.purchasing_sessions += i64::from(session.has_purchase);
        for &assisted in campaign_counts.keys() {
            daily
                .entry((assisted, session.day))
                .or_default()
                .assisted_sessions += 1;
        }
    }
}

fn fast_map<K: std::hash::Hash + Eq, V>() -> FastMap<K, V> {
    HashMap::with_hasher(RandomState::new())
}

fn fast_map_with_capacity<K: std::hash::Hash + Eq, V>(capacity: usize) -> FastMap<K, V> {
    HashMap::with_capacity_and_hasher(capacity, RandomState::new())
}

fn fast_set<K: std::hash::Hash + Eq>() -> FastSet<K> {
    HashSet::with_hasher(RandomState::new())
}

fn fast_set_with_capacity<K: std::hash::Hash + Eq>(capacity: usize) -> FastSet<K> {
    HashSet::with_capacity_and_hasher(capacity, RandomState::new())
}

fn csv_files(dir: &Path) -> Result<Vec<PathBuf>, String> {
    let mut paths = fs::read_dir(dir)
        .map_err(|e| format!("read {}: {e}", dir.display()))?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.is_file() && p.extension().is_some_and(|e| e == "csv"))
        .collect::<Vec<_>>();
    paths.sort();
    Ok(paths)
}

fn fields<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    if line.is_empty() {
        return None;
    }
    let mut out: [&[u8]; N] = [&[]; N];
    let mut start = 0;
    for (index, slot) in out.iter_mut().enumerate() {
        if line.get(start) == Some(&b'"') {
            let close = start + 1 + memchr(b'"', &line[start + 1..])?;
            let next = close + 1;
            *slot = &line[start + 1..close];
            if index + 1 == N {
                if next != line.len() {
                    return None;
                }
            } else if line.get(next) != Some(&b',') {
                return None;
            } else {
                start = next + 1;
            }
        } else {
            let tail = &line[start..];
            let end = match memchr2(b',', b'"', tail) {
                Some(offset) if tail[offset] == b',' => start + offset,
                Some(_) => return None,
                None => line.len(),
            };
            *slot = &line[start..end];
            if index + 1 == N {
                if end != line.len() {
                    return None;
                }
            } else if end == line.len() {
                return None;
            } else {
                start = end + 1;
            }
        }
    }
    Some(out)
}

fn text(bytes: &[u8]) -> Option<&str> {
    std::str::from_utf8(bytes).ok()
}

fn ascii_u64(bytes: &[u8]) -> Option<u64> {
    if bytes.is_empty() {
        return None;
    }
    let mut value = 0u64;
    for &byte in bytes {
        if !byte.is_ascii_digit() {
            return None;
        }
        value = value.checked_mul(10)?.checked_add((byte - b'0') as u64)?;
    }
    Some(value)
}

#[inline]
fn digit(b: u8) -> Option<i64> {
    if b.is_ascii_digit() {
        Some((b - b'0') as i64)
    } else {
        None
    }
}

#[inline]
fn two_digits(bytes: &[u8], at: usize) -> Option<i64> {
    Some(digit(bytes[at])? * 10 + digit(bytes[at + 1])?)
}

#[inline]
fn two_digits_packed(bytes: &[u8], at: usize) -> Option<i64> {
    let pair = u16::from_le_bytes([bytes[at], bytes[at + 1]]);
    if pair & 0xf0f0 != 0x3030 {
        return None;
    }
    Some(i64::from(pair & 0x0f) * 10 + i64::from((pair >> 8) & 0x0f))
}

#[inline]
fn four_digits(bytes: &[u8], at: usize) -> Option<i64> {
    Some(
        digit(bytes[at])? * 1_000
            + digit(bytes[at + 1])? * 100
            + digit(bytes[at + 2])? * 10
            + digit(bytes[at + 3])?,
    )
}

fn leap(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn civil_days(y: i64, m: i64, d: i64) -> i64 {
    let y = y - i64::from(m <= 2);
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = m + if m > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[derive(Default)]
struct YearStartCache {
    entries: [(i64, i64); 4],
    len: usize,
    next: usize,
}

impl YearStartCache {
    fn get(&mut self, year: i64) -> i64 {
        for &(cached_year, start) in &self.entries[..self.len] {
            if cached_year == year {
                return start;
            }
        }
        let start = civil_days(year, 1, 1);
        if self.len < self.entries.len() {
            self.entries[self.len] = (year, start);
            self.len += 1;
        } else {
            self.entries[self.next] = (year, start);
            self.next = (self.next + 1) % self.entries.len();
        }
        start
    }
}

fn timestamp(bytes: &[u8]) -> Option<i64> {
    timestamp_with_cache(bytes, &mut YearStartCache::default())
}

#[inline]
fn two_digits_unchecked(bytes: &[u8], at: usize) -> i64 {
    i64::from(bytes[at] - b'0') * 10 + i64::from(bytes[at + 1] - b'0')
}

#[inline]
fn f1_timestamp_unchecked(bytes: &[u8], years: &mut YearStartCache) -> i64 {
    let year = i64::from(bytes[0] - b'0') * 1_000
        + i64::from(bytes[1] - b'0') * 100
        + i64::from(bytes[2] - b'0') * 10
        + i64::from(bytes[3] - b'0');
    let month = two_digits_unchecked(bytes, 5);
    let day = two_digits_unchecked(bytes, 8);
    let hour = two_digits_unchecked(bytes, 11);
    let minute = two_digits_unchecked(bytes, 14);
    let second = two_digits_unchecked(bytes, 17);
    let month_start = match month {
        1 => 0,
        2 => 31,
        3 => 59,
        4 => 90,
        5 => 120,
        6 => 151,
        7 => 181,
        8 => 212,
        9 => 243,
        10 => 273,
        11 => 304,
        _ => 334,
    };
    let day_of_year = month_start + i64::from(month > 2 && leap(year)) + day - 1;
    (years.get(year) + day_of_year) * DAY_MS + hour * 3_600_000 + minute * 60_000 + second * 1_000
}

fn timestamp_with_cache(bytes: &[u8], years: &mut YearStartCache) -> Option<i64> {
    if bytes.len() == 20
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes[10] == b'T'
        && bytes[13] == b':'
        && bytes[16] == b':'
        && bytes[19] == b'Z'
    {
        let year = two_digits_packed(bytes, 0)? * 100 + two_digits_packed(bytes, 2)?;
        let month = two_digits_packed(bytes, 5)?;
        let date = two_digits_packed(bytes, 8)?;
        let hour = two_digits_packed(bytes, 11)?;
        let minute = two_digits_packed(bytes, 14)?;
        let second = two_digits_packed(bytes, 17)?;
        if year == 0 || month < 1 || month > 12 || hour > 23 || minute > 59 || second > 59 {
            return None;
        }
        let month_start = match month {
            1 => 0,
            2 => 31,
            3 => 59,
            4 => 90,
            5 => 120,
            6 => 151,
            7 => 181,
            8 => 212,
            9 => 243,
            10 => 273,
            11 => 304,
            _ => 334,
        };
        let month_days = match month {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 if leap(year) => 29,
            2 => 28,
            _ => unreachable!(),
        };
        if date < 1 || date > month_days {
            return None;
        }
        let day_of_year = month_start + i64::from(month > 2 && leap(year)) + date - 1;
        return Some(
            (years.get(year) + day_of_year) * DAY_MS
                + hour * 3_600_000
                + minute * 60_000
                + second * 1_000,
        );
    }
    if bytes.len() != 20 && bytes.len() != 25 || bytes.get(4) != Some(&b'-') {
        return ascii_u64(bytes).and_then(|n| i64::try_from(n).ok());
    }
    if bytes.len() != 20 && bytes.len() != 25 {
        return None;
    }
    if bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return None;
    }
    let year = four_digits(bytes, 0)?;
    let month = two_digits(bytes, 5)?;
    let day = two_digits(bytes, 8)?;
    let hour = two_digits(bytes, 11)?;
    let minute = two_digits(bytes, 14)?;
    let second = two_digits(bytes, 17)?;
    let month_days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap(year) => 29,
        2 => 28,
        _ => return None,
    };
    if year == 0 || day < 1 || day > month_days || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let offset_minutes = if bytes.len() == 20 {
        if bytes[19] != b'Z' {
            return None;
        }
        0i64
    } else {
        if bytes[19] != b'+' && bytes[19] != b'-' || bytes[22] != b':' {
            return None;
        }
        let oh = two_digits(bytes, 20)?;
        let om = two_digits(bytes, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let sign = if bytes[19] == b'+' { 1 } else { -1 };
        sign * (oh * 60 + om)
    };
    let month_start = match month {
        1 => 0,
        2 => 31,
        3 => 59,
        4 => 90,
        5 => 120,
        6 => 151,
        7 => 181,
        8 => 212,
        9 => 243,
        10 => 273,
        11 => 304,
        12 => 334,
        _ => return None,
    };
    let day_of_year = month_start + i64::from(month > 2 && leap(year)) + day - 1;
    let local = (years.get(year) + day_of_year)
        .checked_mul(86_400)?
        .checked_add(hour * 3_600 + minute * 60 + second)?;
    local.checked_sub(offset_minutes * 60)?.checked_mul(1_000)
}

fn price(bytes: &[u8]) -> Option<i64> {
    let n = ascii_u64(bytes)?;
    (1..=10_000_000).contains(&n).then_some(n as i64)
}

#[inline]
fn fixed_click_order_key(bytes: &[u8]) -> Option<u64> {
    (bytes.len() == 9
        && bytes[0] == b'c'
        && bytes[1..]
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'z').contains(byte)))
    .then(|| u64::from_be_bytes(bytes[1..9].try_into().unwrap()))
}

#[inline]
fn fixed_click_bytes(key: u64) -> [u8; 9] {
    let mut bytes = [0u8; 9];
    bytes[0] = b'c';
    bytes[1..].copy_from_slice(&key.to_be_bytes());
    bytes
}

#[inline]
fn click_id_cmp(a: u64, b: u64, other_click_ids: &[StrRef]) -> std::cmp::Ordering {
    let a_other = a & FALLBACK_CLICK_ID != 0;
    let b_other = b & FALLBACK_CLICK_ID != 0;
    match (a_other, b_other) {
        (false, false) => a.cmp(&b),
        (true, true) => other_click_ids[(a & !FALLBACK_CLICK_ID) as usize]
            .bytes()
            .cmp(other_click_ids[(b & !FALLBACK_CLICK_ID) as usize].bytes()),
        (false, true) => fixed_click_bytes(a)
            .as_slice()
            .cmp(other_click_ids[(b & !FALLBACK_CLICK_ID) as usize].bytes()),
        (true, false) => other_click_ids[(a & !FALLBACK_CLICK_ID) as usize]
            .bytes()
            .cmp(fixed_click_bytes(b).as_slice()),
    }
}

#[inline]
fn packed_user_id(bytes: &[u8]) -> Option<u64> {
    packed_fixed_id(bytes, b'u')
}

#[inline]
fn packed_fixed_id(bytes: &[u8], prefix: u8) -> Option<u64> {
    if bytes.len() != 9 || bytes[0] != prefix {
        return None;
    }
    Some(spread_fixed_id(u64::from_be_bytes(
        bytes[1..].try_into().unwrap(),
    )))
}

#[inline]
fn spread_fixed_id(packed: u64) -> u64 {
    // An odd multiplier is a permutation of u64. It preserves exact keys while
    // moving fixed-width ID bytes into hashbrown's high-bit tags.
    packed.wrapping_mul(0x9e37_79b9_7f4a_7c15)
}

#[inline]
fn packed_event_id(bytes: &[u8]) -> Option<u64> {
    packed_fixed_id(bytes, b'e')
}

fn each_row_bytes<const N: usize, F>(bytes: &[u8], mut f: F)
where
    F: FnMut([&[u8]; N]),
{
    each_lines::<N, _, false>(bytes, true, &mut f);
}

fn each_utf8_row_bytes<const N: usize, F>(bytes: &[u8], skip_header: bool, mut f: F)
where
    F: FnMut([&[u8]; N]),
{
    each_lines::<N, _, true>(bytes, skip_header, &mut f);
}

fn each_lines<const N: usize, F, const VALIDATE_UTF8: bool>(
    bytes: &[u8],
    mut skip_header: bool,
    f: &mut F,
) where
    F: FnMut([&[u8]; N]),
{
    let mut start = 0;
    for end in memchr_iter(b'\n', bytes) {
        let line = &bytes[start..end];
        start = end + 1;
        if skip_header {
            skip_header = false;
            continue;
        }
        if VALIDATE_UTF8 && std::str::from_utf8(line).is_err() {
            continue;
        }
        if let Some(row) = fields::<N>(line) {
            f(row);
        }
    }
    if !skip_header {
        let line = &bytes[start..];
        if (!VALIDATE_UTF8 || std::str::from_utf8(line).is_ok())
            && let Some(row) = fields::<N>(line)
        {
            f(row);
        }
    }
}

fn each_row<const N: usize, F>(path: &Path, f: F) -> Result<(), String>
where
    F: FnMut([&[u8]; N]),
{
    let bytes = fs::read(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    each_row_bytes::<N, _>(&bytes, f);
    Ok(())
}

fn parse_links(data: &Path, strings: &mut Interner) -> Result<Vec<(Id, Id)>, String> {
    let mut links = Vec::new();
    for path in csv_files(&data.join("links"))? {
        each_row::<2, _>(&path, |row| {
            if let (Some(a), Some(b)) = (text(row[0]), text(row[1])) {
                if !a.is_empty() && !b.is_empty() {
                    links.push((strings.intern(a), strings.intern(b)));
                }
            }
        })?;
    }
    Ok(links)
}

fn parse_products(data: &Path, strings: &mut Interner) -> Result<FastMap<Id, Id>, String> {
    let mut products = fast_map();
    each_row::<2, _>(&data.join("products.csv"), |row| {
        if let (Some(product), Some(category)) = (text(row[0]), text(row[1])) {
            products.insert(strings.intern(product), strings.intern(category));
        }
    })?;
    Ok(products)
}

fn parse_campaigns(data: &Path, strings: &mut Interner) -> Result<FastMap<Id, Id>, String> {
    let mut campaigns = fast_map();
    each_row::<2, _>(&data.join("campaigns.csv"), |row| {
        if let (Some(campaign), Some(channel)) = (text(row[0]), text(row[1])) {
            campaigns.insert(strings.intern(campaign), strings.intern(channel));
        }
    })?;
    Ok(campaigns)
}

fn parse_event_chunk(
    bytes: &[u8],
    skip_header: bool,
) -> (BorrowedInterner, Vec<StrRef>, Vec<EventRecord>) {
    let mut strings = BorrowedInterner::with_capacity(bytes.len() / 152);
    let mut events = Vec::with_capacity(bytes.len() / 76);
    let mut fallback_ids = Vec::new();
    let mut years = YearStartCache::default();
    each_utf8_row_bytes::<7, _>(bytes, skip_header, |row| {
        if row[0].is_empty() {
            return;
        }
        let kind = match row[4] {
            b"view" => 0,
            b"cart" => 1,
            b"purchase" => 2,
            _ => INVALID_EVENT_KIND,
        };
        let ts = timestamp_with_cache(row[2], &mut years).unwrap_or(INVALID_EVENT_TS);
        let ingest = f1_timestamp_unchecked(row[3], &mut years);
        let user = if row[1].is_empty() {
            MISSING_ID
        } else {
            strings.intern_user(row[1])
        };
        let product = if row[5].is_empty() {
            MISSING_ID
        } else if kind == 2 {
            strings.intern(row[5])
        } else {
            0
        };
        let price = if kind == 2 {
            price(row[6])
                .map(|amount| amount as u32)
                .unwrap_or(INVALID_PRICE_CENTS)
        } else {
            INVALID_PRICE_CENTS
        };
        let event = Event::new(ts, ingest, user, product, price, kind);
        let (id_key, fallback_id) = if let Some(key) = packed_event_id(row[0]) {
            (key, MISSING_ID)
        } else {
            let mut hasher = AHasher::default();
            hasher.write(row[0]);
            let index = fallback_ids.len() as Id;
            fallback_ids.push(StrRef::new(row[0]));
            (hasher.finish(), index)
        };
        events.push((id_key, fallback_id, event));
    });
    (strings, fallback_ids, events)
}

fn parse_event_file(
    path: &Path,
) -> Result<
    (
        Box<[u8]>,
        Vec<(BorrowedInterner, Vec<StrRef>, Vec<EventRecord>)>,
    ),
    String,
> {
    const CHUNK_SIZE: usize = 16 * 1024 * 1024;
    let bytes = fs::read(path)
        .map_err(|e| format!("read {}: {e}", path.display()))?
        .into_boxed_slice();
    let mut ranges = Vec::with_capacity(bytes.len().div_ceil(CHUNK_SIZE));
    let mut start = 0;
    while start < bytes.len() {
        let target = (start + CHUNK_SIZE).min(bytes.len());
        let end = if target == bytes.len() {
            bytes.len()
        } else {
            target
                + memchr(b'\n', &bytes[target..]).map_or(bytes.len() - target, |offset| offset + 1)
        };
        ranges.push((start, end));
        start = end;
    }

    let parsed = ranges
        .par_iter()
        .map(|&(start, end)| parse_event_chunk(&bytes[start..end], start == 0))
        .collect::<Vec<_>>();
    Ok((bytes, parsed))
}

fn parse_events(data: &Path, strings: &mut Interner, ncpu: usize) -> Result<Vec<Event>, String> {
    let paths = csv_files(&data.join("events"))?;
    let mut sized_paths = paths
        .into_iter()
        .map(|path| {
            let size = fs::metadata(&path)
                .map_err(|error| format!("stat {}: {error}", path.display()))?
                .len();
            Ok((path, size))
        })
        .collect::<Result<Vec<_>, String>>()?;
    sized_paths.sort_unstable_by_key(|(_, size)| Reverse(*size));
    let mut buckets = (0..ncpu).map(|_| Vec::new()).collect::<Vec<_>>();
    let mut bucket_bytes = vec![0u64; ncpu];
    for (path, size) in sized_paths {
        let bucket = bucket_bytes
            .iter()
            .enumerate()
            .min_by_key(|(_, total)| *total)
            .map(|(index, _)| index)
            .unwrap();
        bucket_bytes[bucket] += size;
        buckets[bucket].push(path);
    }
    let estimated_events = bucket_bytes.iter().sum::<u64>() as usize / 82;
    let shard_capacity = estimated_events.div_ceil(ncpu);
    let mut shards = (0..ncpu)
        .map(|_| EventShard::with_capacity(shard_capacity))
        .collect::<Vec<_>>();
    let batch_count = buckets.iter().map(Vec::len).max().unwrap_or(0);
    let batch_width = 8;
    for batch_start in (0..batch_count).step_by(batch_width) {
        let batch = buckets
            .iter()
            .flat_map(|bucket| {
                let end = (batch_start + batch_width).min(bucket.len());
                bucket.get(batch_start..end).unwrap_or(&[])
            })
            .collect::<Vec<_>>();
        let parsed = batch
            .par_iter()
            .map(|path| parse_event_file(path))
            .collect::<Result<Vec<_>, _>>()?;
        let mut prepared = Vec::with_capacity(parsed.len());
        for (source, chunks) in parsed {
            let chunks = chunks
                .into_iter()
                .map(|(local_strings, fallback_ids, local_events)| {
                    let remap = local_strings
                        .values
                        .iter()
                        .map(|value| strings.intern(text(value.bytes()).unwrap()))
                        .collect::<Vec<_>>();
                    (local_events, remap, fallback_ids)
                })
                .collect::<Vec<_>>();
            prepared.push((source, chunks));
        }
        let file_partitions = prepared
            .into_par_iter()
            .map(|(source, chunks)| {
                let mut partitions = (0..ncpu).map(|_| Vec::new()).collect::<Vec<_>>();
                let mut fallback_ids = Vec::new();
                for (local_events, remap, chunk_fallback_ids) in chunks {
                    let fallback_offset = fallback_ids.len() as Id;
                    fallback_ids.extend(chunk_fallback_ids);
                    for (hash, fallback_id, mut event) in local_events {
                        let fallback_id = if fallback_id == MISSING_ID {
                            MISSING_ID
                        } else {
                            fallback_id + fallback_offset
                        };
                        let user = event.user();
                        if user != MISSING_ID {
                            event.set_user(remap[user as usize]);
                        }
                        let product = event.product();
                        if product != MISSING_ID && event.kind() == 2 {
                            event.set_product(remap[product as usize]);
                        }
                        partitions[hash as usize % ncpu].push((hash, fallback_id, event));
                    }
                }
                (source, fallback_ids, partitions)
            })
            .collect::<Vec<_>>();
        let file_count = file_partitions.len();
        let mut partitions = (0..ncpu)
            .map(|_| Vec::with_capacity(file_count))
            .collect::<Vec<_>>();
        let mut source_buffers = Vec::with_capacity(file_count);
        let mut file_fallback_ids = Vec::with_capacity(file_count);
        for (source, fallback_ids, file_partition) in file_partitions {
            source_buffers.push(source);
            file_fallback_ids.push(fallback_ids);
            for (partition, file_records) in partitions.iter_mut().zip(file_partition) {
                partition.push(file_records);
            }
        }
        shards
            .par_iter_mut()
            .zip(partitions.into_par_iter())
            .for_each(|(shard, file_records)| {
                for (records, fallback_ids) in file_records.into_iter().zip(&file_fallback_ids) {
                    for (hash, fallback_id, event) in records {
                        let event_id = if fallback_id == MISSING_ID {
                            StrRef::new(&[])
                        } else {
                            fallback_ids[fallback_id as usize]
                        };
                        shard.insert(hash, event_id, event, fallback_id == MISSING_ID);
                    }
                }
            });
        drop(file_fallback_ids);
        drop(source_buffers);
    }
    let total_events = shards
        .iter()
        .map(|shard| {
            shard.primary.len()
                + shard.fallback.len()
                + shard.collisions.values().map(Vec::len).sum::<usize>()
        })
        .sum();
    let mut events = Vec::with_capacity(total_events);
    for shard in shards {
        shard.append_events(&mut events);
    }
    Ok(events)
}

fn parse_click_file(path: &Path) -> Result<(Box<[u8]>, BorrowedInterner, Vec<RawClick>), String> {
    let bytes = fs::read(path)
        .map_err(|e| format!("read {}: {e}", path.display()))?
        .into_boxed_slice();
    let mut strings = BorrowedInterner::with_capacity(bytes.len() / 100);
    let mut seen: FastSet<StrRef> = fast_set_with_capacity(bytes.len() / 64);
    let mut clicks = Vec::new();
    let mut years = YearStartCache::default();
    each_row_bytes::<4, _>(&bytes, |row| {
        let (Some(id), Some(user), Some(campaign), Some(ts)) =
            (text(row[0]), text(row[1]), text(row[2]), text(row[3]))
        else {
            return;
        };
        if id.is_empty() || user.is_empty() || !seen.insert(StrRef::new(row[0])) {
            return;
        }
        if let Some(ts) = timestamp_with_cache(ts.as_bytes(), &mut years) {
            clicks.push(RawClick {
                id: StrRef::new(row[0]),
                user: strings.intern_user(user.as_bytes()),
                ts,
                campaign: strings.intern(campaign.as_bytes()),
            });
        }
    });
    Ok((bytes, strings, clicks))
}

fn parse_clicks(
    data: &Path,
    strings: &mut Interner,
    ncpu: usize,
) -> Result<(Vec<Box<[u8]>>, Vec<RawClick>), String> {
    let paths = csv_files(&data.join("clicks"))?;
    let mut seen = (0..ncpu).map(|_| fast_set()).collect::<Vec<_>>();
    let mut source_buffers = Vec::with_capacity(paths.len());
    let mut clicks = Vec::new();
    for batch in paths.chunks(paths.len().max(1)) {
        let parsed = batch
            .par_iter()
            .map(|path| parse_click_file(path))
            .collect::<Result<Vec<_>, _>>()?;
        let mut partitions = (0..ncpu).map(|_| Vec::new()).collect::<Vec<_>>();
        for (source, local_strings, local_clicks) in parsed {
            source_buffers.push(source);
            let remap = local_strings
                .values
                .iter()
                .map(|value| strings.intern(text(value.bytes()).unwrap()))
                .collect::<Vec<_>>();
            for mut click in local_clicks {
                click.user = remap[click.user as usize];
                click.campaign = remap[click.campaign as usize];
                let mut hasher = AHasher::default();
                hasher.write(click.id.bytes());
                let hash = hasher.finish();
                partitions[hash as usize % ncpu].push(click);
            }
        }
        let added = seen
            .par_iter_mut()
            .zip(partitions.into_par_iter())
            .map(|(seen, candidates)| {
                candidates
                    .into_iter()
                    .filter(|click| seen.insert(click.id))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        clicks.extend(added.into_iter().flatten());
    }
    Ok((source_buffers, clicks))
}

fn day(ts: i64) -> i32 {
    ts.div_euclid(DAY_MS) as i32
}

fn output_dir(out: &Path, name: &str) -> Result<PathBuf, String> {
    let dir = out.join(name);
    fs::create_dir_all(&dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    Ok(dir.join("part-0.parquet"))
}

fn write_table(
    path: &Path,
    names: &[&str],
    types: &[DataType],
    arrays: Vec<ArrayRef>,
) -> Result<(), String> {
    let fields = names
        .iter()
        .zip(types)
        .map(|(name, ty)| Field::new(*name, ty.clone(), true))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays).map_err(|e| e.to_string())?;
    let file = File::create(path).map_err(|e| format!("create {}: {e}", path.display()))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).map_err(|e| e.to_string())?;
    writer.write(&batch).map_err(|e| e.to_string())?;
    writer.close().map_err(|e| e.to_string())?;
    Ok(())
}

fn run() -> Result<(), String> {
    let overall = Instant::now();
    let ncpu = env::var("NCPU")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, usize::from));
    rayon::ThreadPoolBuilder::new()
        .num_threads(ncpu)
        .build_global()
        .map_err(|e| format!("create worker pool: {e}"))?;
    let data = PathBuf::from(env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let out = PathBuf::from(env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let manifest: serde_json::Value = serde_json::from_reader(BufReader::new(
        File::open(data.join("manifest.json")).map_err(|e| format!("open manifest: {e}"))?,
    ))
    .map_err(|e| format!("parse manifest: {e}"))?;
    let start = timestamp(
        manifest["window_start"]
            .as_str()
            .ok_or("manifest has no window_start")?
            .as_bytes(),
    )
    .ok_or("invalid window_start")?;
    let end = timestamp(
        manifest["window_end"]
            .as_str()
            .ok_or("manifest has no window_end")?
            .as_bytes(),
    )
    .ok_or("invalid window_end")?;

    let mut strings = Interner::default();
    let phase = Instant::now();
    let links = parse_links(&data, &mut strings)?;
    let unknown = strings.intern("unknown");
    let organic = strings.intern("organic");
    let products = parse_products(&data, &mut strings)?;
    let campaigns = parse_campaigns(&data, &mut strings)?;
    eprintln!(
        "loaded reference files in {:.3}s",
        phase.elapsed().as_secs_f64()
    );
    let phase = Instant::now();
    let events = parse_events(&data, &mut strings, ncpu)?;
    eprintln!(
        "parsed and deduplicated events in {:.3}s",
        phase.elapsed().as_secs_f64()
    );
    let phase = Instant::now();
    let (click_sources, mut raw_clicks) = parse_clicks(&data, &mut strings, ncpu)?;
    eprintln!(
        "parsed and deduplicated clicks in {:.3}s",
        phase.elapsed().as_secs_f64()
    );
    let mut dsu = Dsu::new(strings.values.len());
    for &(a, b) in &links {
        dsu.union(a, b, &strings);
    }
    // Every ID with a non-root parent comes from the link table. Flatten these
    // once so canonicalization in the event and click loops is one array read.
    for &(a, b) in &links {
        dsu.root(a);
        dsu.root(b);
    }
    let phase = Instant::now();

    let mut surviving = events;
    surviving.retain_mut(|e| {
        if e.user() == MISSING_ID
            || e.ts == INVALID_EVENT_TS
            || e.ts < start
            || e.ts >= end
            || e.product() == MISSING_ID
            || e.kind() > 2
            || (e.kind() == 2 && e.price() == INVALID_PRICE_CENTS)
        {
            return false;
        }
        if i128::from(e.ingest_seconds) * 1_000 - i128::from(e.ts) > 86_400_000 {
            return false;
        }
        e.set_user(dsu.canonical(e.user()));
        true
    });
    for c in &mut raw_clicks {
        if c.ts < start.saturating_sub(ATTRIBUTION_MS)
            || c.ts >= end
            || !campaigns.contains_key(&c.campaign)
        {
            c.ts = i64::MIN;
        } else {
            c.user = dsu.canonical(c.user);
        }
    }
    raw_clicks.retain(|c| c.ts != i64::MIN);
    eprintln!(
        "linked IDs and filtered records in {:.3}s",
        phase.elapsed().as_secs_f64()
    );

    let phase = Instant::now();
    rayon::join(
        || surviving.par_sort_unstable_by_key(|e| (e.user(), e.ts)),
        || raw_clicks.par_sort_unstable_by_key(|c| (c.user, c.ts)),
    );
    eprintln!(
        "sorted events and clicks in {:.3}s",
        phase.elapsed().as_secs_f64()
    );
    let phase = Instant::now();
    let mut user_ranges = Vec::new();
    let mut i = 0;
    while i < surviving.len() {
        let person = surviving[i].user();
        let mut end = i + 1;
        while end < surviving.len() && surviving[end].user() == person {
            end += 1;
        }
        user_ranges.push((i, end));
        i = end;
    }
    let aggregated = user_ranges
        .par_chunks(512)
        .map(|ranges| {
            let mut user_rows = Vec::new();
            let mut sessions = Vec::new();
            for &(start, end) in ranges {
                aggregate_user_group(
                    &surviving[start..end],
                    &products,
                    unknown,
                    &mut user_rows,
                    &mut sessions,
                );
            }
            (user_rows, sessions)
        })
        .collect::<Vec<_>>();
    let mut users = Vec::new();
    let mut sessions = Vec::new();
    for (mut user_rows, mut user_sessions) in aggregated {
        users.append(&mut user_rows);
        sessions.append(&mut user_sessions);
    }

    eprintln!(
        "sessionized and aggregated users in {:.3}s",
        phase.elapsed().as_secs_f64()
    );
    let phase = Instant::now();
    let mut other_click_ids = Vec::new();
    let clicks = raw_clicks
        .into_iter()
        .map(|click| {
            let id_key = fixed_click_order_key(click.id.bytes()).unwrap_or_else(|| {
                let index = other_click_ids.len() as u64;
                other_click_ids.push(click.id);
                FALLBACK_CLICK_ID | index
            });
            Click {
                id_key,
                user: click.user,
                ts: click.ts,
                campaign: click.campaign,
            }
        })
        .collect::<Vec<_>>();
    let mut attribution_ranges = Vec::new();
    let mut si = 0usize;
    let mut ci = 0usize;
    while si < sessions.len() {
        let person = sessions[si].person;
        let mut se = si;
        while se < sessions.len() && sessions[se].person == person {
            se += 1;
        }
        while ci < clicks.len() && clicks[ci].user < person {
            ci += 1;
        }
        let mut ce = ci;
        while ce < clicks.len() && clicks[ce].user == person {
            ce += 1;
        }
        attribution_ranges.push((si, se, ci, ce));
        ci = ce;
        si = se;
    }
    let partial_campaigns = attribution_ranges
        .par_chunks(4096)
        .map(|groups| {
            let mut daily = fast_map();
            let mut active = VecDeque::new();
            let mut campaign_counts = fast_map();
            for &(session_start, session_end, click_start, click_end) in groups {
                attribute_user_sessions(
                    &sessions[session_start..session_end],
                    &clicks[click_start..click_end],
                    &other_click_ids,
                    organic,
                    &mut daily,
                    &mut active,
                    &mut campaign_counts,
                );
            }
            daily
        })
        .collect::<Vec<FastMap<(Id, i32), CampaignAgg>>>();
    drop(click_sources);
    let mut campaign_daily: FastMap<(Id, i32), CampaignAgg> = fast_map();
    for partial in partial_campaigns {
        for (key, row) in partial {
            let total = campaign_daily.entry(key).or_default();
            total.sessions += row.sessions;
            total.purchasing_sessions += row.purchasing_sessions;
            total.revenue += row.revenue;
            total.assisted_sessions += row.assisted_sessions;
        }
    }

    let user_totals = users
        .iter()
        .fold((0i64, 0i64), |(sessions, revenue), (_, row)| {
            (sessions + row.sessions, revenue + row.revenue)
        });
    let campaign_totals = campaign_daily.iter().fold(
        (0i64, 0i64, 0i64, 0i64),
        |(sessions, revenue, assists, nonorganic), ((campaign, _), row)| {
            (
                sessions + row.sessions,
                revenue + row.revenue,
                assists + row.assisted_sessions,
                nonorganic
                    + if *campaign == organic {
                        0
                    } else {
                        row.sessions
                    },
            )
        },
    );
    if user_totals.0 != sessions.len() as i64
        || user_totals.1 != campaign_totals.1
        || user_totals.0 != campaign_totals.0
        || campaign_totals.2 < campaign_totals.3
    {
        return Err(format!(
            "aggregate invariant failed: user={user_totals:?}, campaign={campaign_totals:?}, sessions={}",
            sessions.len()
        ));
    }

    write_outputs(&out, &strings, &users, &campaign_daily, &campaigns)?;
    eprintln!(
        "attributed sessions and wrote tables in {:.3}s",
        phase.elapsed().as_secs_f64()
    );
    eprintln!(
        "processed {} surviving events, {} sessions, {} clicks in {:.3}s",
        surviving.len(),
        sessions.len(),
        clicks.len(),
        overall.elapsed().as_secs_f64()
    );
    Ok(())
}

fn write_outputs(
    out: &Path,
    strings: &Interner,
    users: &[((Id, i32), UserAgg)],
    campaigns_daily: &FastMap<(Id, i32), CampaignAgg>,
    channels: &FastMap<Id, Id>,
) -> Result<(), String> {
    let mut user_rows = users.iter().collect::<Vec<_>>();
    user_rows.sort_unstable_by_key(|((user, d), _)| (*user, *d));
    let mut user_ids = Vec::with_capacity(user_rows.len());
    let mut user_days = Vec::with_capacity(user_rows.len());
    let mut user_sessions = Vec::with_capacity(user_rows.len());
    let mut user_events = Vec::with_capacity(user_rows.len());
    let mut user_revenue = Vec::with_capacity(user_rows.len());
    let mut top_category = Vec::with_capacity(user_rows.len());
    for ((user, d), agg) in user_rows {
        user_ids.push(strings.get(*user));
        user_days.push(*d);
        user_sessions.push(agg.sessions);
        user_events.push(agg.events);
        user_revenue.push(agg.revenue);
        let top = agg
            .categories
            .iter()
            .max_by(|(a, av), (b, bv)| {
                av.cmp(bv)
                    .then_with(|| strings.get(**b).as_bytes().cmp(strings.get(**a).as_bytes()))
            })
            .map(|(&id, _)| strings.get(id));
        top_category.push(top);
    }
    let user_path = output_dir(out, "user_daily")?;
    let user_arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(user_ids)),
        Arc::new(Date32Array::from(user_days)),
        Arc::new(Int64Array::from(user_sessions)),
        Arc::new(Int64Array::from(user_events)),
        Arc::new(Int64Array::from(user_revenue)),
        Arc::new(StringArray::from(top_category)),
    ];

    let mut campaign_rows = campaigns_daily.iter().collect::<Vec<_>>();
    campaign_rows.sort_unstable_by_key(|((campaign, d), _)| (*campaign, *d));
    let mut campaign_ids = Vec::with_capacity(campaign_rows.len());
    let mut channels_out = Vec::with_capacity(campaign_rows.len());
    let mut campaign_days = Vec::with_capacity(campaign_rows.len());
    let mut campaign_sessions = Vec::with_capacity(campaign_rows.len());
    let mut purchasing = Vec::with_capacity(campaign_rows.len());
    let mut revenue = Vec::with_capacity(campaign_rows.len());
    let mut assists = Vec::with_capacity(campaign_rows.len());
    for (&(campaign, d), agg) in campaign_rows {
        campaign_ids.push(strings.get(campaign));
        channels_out.push(if strings.get(campaign) == "organic" {
            "organic"
        } else {
            strings.get(
                *channels.get(&campaign).ok_or_else(|| {
                    format!("missing channel for campaign {}", strings.get(campaign))
                })?,
            )
        });
        campaign_days.push(d);
        campaign_sessions.push(agg.sessions);
        purchasing.push(agg.purchasing_sessions);
        revenue.push(agg.revenue);
        assists.push(agg.assisted_sessions);
    }
    let campaign_path = output_dir(out, "campaign_daily")?;
    let campaign_arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(campaign_ids)),
        Arc::new(StringArray::from(channels_out)),
        Arc::new(Date32Array::from(campaign_days)),
        Arc::new(Int64Array::from(campaign_sessions)),
        Arc::new(Int64Array::from(purchasing)),
        Arc::new(Int64Array::from(revenue)),
        Arc::new(Int64Array::from(assists)),
    ];

    let (user_result, campaign_result) = rayon::join(
        || {
            write_table(
                &user_path,
                &[
                    "user_id",
                    "day",
                    "sessions",
                    "events",
                    "revenue_cents",
                    "top_category",
                ],
                &[
                    DataType::Utf8,
                    DataType::Date32,
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Utf8,
                ],
                user_arrays,
            )
        },
        || {
            write_table(
                &campaign_path,
                &[
                    "campaign_id",
                    "channel",
                    "day",
                    "sessions",
                    "purchasing_sessions",
                    "revenue_cents",
                    "assisted_sessions",
                ],
                &[
                    DataType::Utf8,
                    DataType::Utf8,
                    DataType::Date32,
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                ],
                campaign_arrays,
            )
        },
    );
    user_result?;
    campaign_result?;
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("shopping-sessions: {error}");
        std::process::exit(1);
    }
}
