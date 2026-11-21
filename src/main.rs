use ahash::{AHasher, RandomState};
use arrow_array::{ArrayRef, Date32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use memchr::memchr_iter;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;
use std::cmp::Reverse;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::env;
use std::fs::{self, File};
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

type Id = u32;
type FastMap<K, V> = HashMap<K, V, RandomState>;
type FastSet<K> = HashSet<K, RandomState>;
type EventMap = hashbrown::HashMap<u64, (StrRef, Event), BuildHasherDefault<IdentityHasher>>;

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

struct EventDeduper {
    primary: EventMap,
    collisions: FastMap<u64, Vec<(StrRef, Event)>>,
}

impl EventDeduper {
    fn new() -> Self {
        Self {
            primary: EventMap::default(),
            collisions: fast_map(),
        }
    }

    fn insert(&mut self, id: StrRef, event: Event) {
        let mut hasher = AHasher::default();
        id.hash(&mut hasher);
        let hash = hasher.finish();
        self.insert_hashed(hash, id, event);
    }

    fn insert_hashed(&mut self, hash: u64, id: StrRef, event: Event) {
        match self
            .primary
            .raw_entry_mut()
            .from_hash(hash, |stored_hash| *stored_hash == hash)
        {
            hashbrown::hash_map::RawEntryMut::Vacant(entry) => {
                entry.insert_hashed_nocheck(hash, hash, (id, event));
            }
            hashbrown::hash_map::RawEntryMut::Occupied(mut entry) => {
                let (stored_id, stored_event) = entry.get_mut();
                if *stored_id == id {
                    if event.ingest < stored_event.ingest {
                        *stored_event = event;
                    }
                    return;
                }
                let collisions = self.collisions.entry(hash).or_default();
                if let Some((_, stored_event)) = collisions
                    .iter_mut()
                    .find(|(candidate, _)| *candidate == id)
                {
                    if event.ingest < stored_event.ingest {
                        *stored_event = event;
                    }
                } else {
                    collisions.push((id, event));
                }
            }
        }
    }

    fn drain_into(self, partitions: &mut [Vec<(u64, StrRef, Event)>], remap: &[Id]) {
        let push = |hash: u64,
                    id: StrRef,
                    mut event: Event,
                    partitions: &mut [Vec<(u64, StrRef, Event)>]| {
            event.user = event.user.map(|id| remap[id as usize]);
            event.product = event.product.map(|id| remap[id as usize]);
            partitions[hash as usize % partitions.len()].push((hash, id, event));
        };
        for (hash, (id, event)) in self.primary {
            push(hash, id, event, partitions);
        }
        for (hash, events) in self.collisions {
            for (id, event) in events {
                push(hash, id, event, partitions);
            }
        }
    }
}

const DAY_MS: i64 = 86_400_000;
const SESSION_GAP_MS: i64 = 1_800_000;
const ATTRIBUTION_MS: i64 = 604_800_000;

#[derive(Default)]
struct Interner {
    values: Vec<Arc<str>>,
    ids: FastMap<Arc<str>, Id>,
}

impl Interner {
    fn intern(&mut self, text: &str) -> Id {
        if let Some(&id) = self.ids.get(text) {
            return id;
        }
        let text: Arc<str> = Arc::from(text);
        let id = self.values.len() as Id;
        self.values.push(text.clone());
        self.ids.insert(text, id);
        id
    }

    fn get(&self, id: Id) -> &str {
        &self.values[id as usize]
    }
}

#[derive(Default)]
struct BorrowedInterner {
    values: Vec<StrRef>,
    ids: FastMap<StrRef, Id>,
}

impl BorrowedInterner {
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
}

struct Dsu {
    parent: Vec<Id>,
    min_id: Vec<Id>,
}

impl Dsu {
    fn new(n: usize) -> Self {
        Self {
            parent: (0..n as Id).collect(),
            min_id: (0..n as Id).collect(),
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
        let ma = self.min_id[ra as usize];
        let mb = self.min_id[rb as usize];
        if strings.get(ma).as_bytes() > strings.get(mb).as_bytes() {
            std::mem::swap(&mut ra, &mut rb);
        }
        self.parent[rb as usize] = ra;
        let ca = self.min_id[ra as usize];
        let cb = self.min_id[rb as usize];
        if strings.get(cb).as_bytes() < strings.get(ca).as_bytes() {
            self.min_id[ra as usize] = cb;
        }
    }

    fn canonical(&mut self, id: Id) -> Id {
        let r = self.root(id);
        self.min_id[r as usize]
    }
}

#[derive(Clone, Copy)]
struct Event {
    user: Option<Id>,
    ts: Option<i64>,
    kind: u8,
    product: Option<Id>,
    price: Option<i64>,
    ingest: i64,
}

#[derive(Clone, Copy)]
struct Click {
    id: Id,
    user: Id,
    ts: i64,
    campaign: Id,
}

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

fn fast_map<K: std::hash::Hash + Eq, V>() -> FastMap<K, V> {
    HashMap::with_hasher(RandomState::new())
}

fn fast_set<K: std::hash::Hash + Eq>() -> FastSet<K> {
    HashSet::with_hasher(RandomState::new())
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
    let mut count = 0usize;
    let mut start = 0;
    for end in memchr_iter(b',', line).chain(std::iter::once(line.len())) {
        let raw = &line[start..end];
        let field = if raw.first() == Some(&b'"') {
            if raw.len() < 2 || raw.last() != Some(&b'"') || raw[1..raw.len() - 1].contains(&b'"') {
                return None;
            }
            &raw[1..raw.len() - 1]
        } else {
            if raw.contains(&b'"') {
                return None;
            }
            raw
        };
        if count == N {
            return None;
        }
        out[count] = field;
        count += 1;
        start = end + 1;
    }
    (count == N).then_some(out)
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

fn timestamp(bytes: &[u8]) -> Option<i64> {
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
    let local = civil_days(year, month, day)
        .checked_mul(86_400)?
        .checked_add(hour * 3_600 + minute * 60 + second)?;
    local.checked_sub(offset_minutes * 60)?.checked_mul(1_000)
}

fn price(bytes: &[u8]) -> Option<i64> {
    let n = ascii_u64(bytes)?;
    (1..=10_000_000).contains(&n).then_some(n as i64)
}

fn each_row_bytes<const N: usize, F>(bytes: &[u8], mut f: F)
where
    F: FnMut([&[u8]; N]),
{
    let mut lines = bytes.split(|b| *b == b'\n');
    let _header = lines.next();
    for line in lines {
        if let Some(row) = fields::<N>(line) {
            f(row);
        }
    }
}

fn each_utf8_row_bytes<const N: usize, F>(bytes: &[u8], mut f: F)
where
    F: FnMut([&[u8]; N]),
{
    let mut lines = bytes.split(|b| *b == b'\n');
    let _header = lines.next();
    for line in lines {
        if std::str::from_utf8(line).is_ok() {
            if let Some(row) = fields::<N>(line) {
                f(row);
            }
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

fn parse_event_file(path: &Path) -> Result<(Box<[u8]>, BorrowedInterner, EventDeduper), String> {
    let bytes = fs::read(path)
        .map_err(|e| format!("read {}: {e}", path.display()))?
        .into_boxed_slice();
    let mut strings = BorrowedInterner::default();
    let mut events = EventDeduper::new();
    each_utf8_row_bytes::<7, _>(&bytes, |row| {
        if row[0].is_empty() {
            return;
        }
        let kind = match row[4] {
            b"view" => 0,
            b"cart" => 1,
            b"purchase" => 2,
            _ => 255,
        };
        let event = Event {
            user: (!row[1].is_empty()).then(|| strings.intern(row[1])),
            ts: timestamp(row[2]),
            kind,
            product: (!row[5].is_empty()).then(|| strings.intern(row[5])),
            price: (kind == 2).then(|| price(row[6])).flatten(),
            ingest: timestamp(row[3]).unwrap_or(i64::MAX),
        };
        events.insert(StrRef::new(row[0]), event);
    });
    Ok((bytes, strings, events))
}

fn parse_events(data: &Path, strings: &mut Interner, ncpu: usize) -> Result<Vec<Event>, String> {
    let paths = csv_files(&data.join("events"))?;
    let mut shards = (0..ncpu).map(|_| EventDeduper::new()).collect::<Vec<_>>();
    let mut source_buffers: Vec<Box<[u8]>> = Vec::with_capacity(paths.len());
    for batch in paths.chunks(ncpu) {
        let parsed = batch
            .par_iter()
            .map(|path| parse_event_file(path))
            .collect::<Result<Vec<_>, _>>()?;
        let mut partitions = (0..ncpu).map(|_| Vec::new()).collect::<Vec<_>>();
        for (source, local_strings, local_events) in parsed {
            source_buffers.push(source);
            let remap = local_strings
                .values
                .iter()
                .map(|value| strings.intern(text(value.bytes()).unwrap()))
                .collect::<Vec<_>>();
            local_events.drain_into(&mut partitions, &remap);
        }
        shards
            .par_iter_mut()
            .zip(partitions.into_par_iter())
            .for_each(|(shard, records)| {
                for (hash, event_id, event) in records {
                    shard.insert_hashed(hash, event_id, event);
                }
            });
    }
    let total_events = shards
        .iter()
        .map(|shard| shard.primary.len() + shard.collisions.values().map(Vec::len).sum::<usize>())
        .sum();
    let mut events = Vec::with_capacity(total_events);
    for shard in shards {
        events.extend(shard.primary.into_values().map(|(_, event)| event));
        events.extend(
            shard
                .collisions
                .into_values()
                .flatten()
                .map(|(_, event)| event),
        );
    }
    drop(source_buffers);
    Ok(events)
}

fn parse_click_file(path: &Path) -> Result<(Box<[u8]>, BorrowedInterner, Vec<RawClick>), String> {
    let bytes = fs::read(path)
        .map_err(|e| format!("read {}: {e}", path.display()))?
        .into_boxed_slice();
    let mut strings = BorrowedInterner::default();
    let mut seen: FastSet<StrRef> = fast_set();
    let mut clicks = Vec::new();
    each_row_bytes::<4, _>(&bytes, |row| {
        let (Some(id), Some(user), Some(campaign), Some(ts)) =
            (text(row[0]), text(row[1]), text(row[2]), text(row[3]))
        else {
            return;
        };
        if id.is_empty() || user.is_empty() || !seen.insert(StrRef::new(row[0])) {
            return;
        }
        if let Some(ts) = timestamp(ts.as_bytes()) {
            clicks.push(RawClick {
                id: StrRef::new(row[0]),
                user: strings.intern(user.as_bytes()),
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
    for batch in paths.chunks(ncpu) {
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
                click.id.hash(&mut hasher);
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
    for (a, b) in links {
        dsu.union(a, b, &strings);
    }
    let phase = Instant::now();

    let mut surviving = Vec::with_capacity(events.len());
    for mut e in events {
        let (Some(user), Some(ts), Some(product)) = (e.user, e.ts, e.product) else {
            continue;
        };
        if ts < start || ts >= end || e.kind > 2 || (e.kind == 2 && e.price.is_none()) {
            continue;
        }
        if (e.ingest as i128) - (ts as i128) > 86_400_000 {
            continue;
        }
        e.user = Some(dsu.canonical(user));
        e.ts = Some(ts);
        let _ = product;
        surviving.push(e);
    }
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
    surviving.par_sort_unstable_by_key(|e| (e.user.unwrap(), e.ts.unwrap()));
    raw_clicks.par_sort_unstable_by_key(|c| (c.user, c.ts));
    eprintln!(
        "sorted events and clicks in {:.3}s",
        phase.elapsed().as_secs_f64()
    );
    let phase = Instant::now();
    let mut users: FastMap<(Id, i32), UserAgg> = fast_map();
    let mut sessions = Vec::new();
    let mut i = 0;
    while i < surviving.len() {
        let first = surviving[i];
        let person = first.user.unwrap();
        let mut session_start = first.ts.unwrap();
        let mut session_day = day(session_start);
        let mut session_revenue = 0i64;
        let mut session_purchase = false;
        let mut previous_ts = session_start;
        let mut j = i;
        while j < surviving.len() && surviving[j].user.unwrap() == person {
            let ts = surviving[j].ts.unwrap();
            let new_session = j == i || ts - previous_ts > SESSION_GAP_MS;
            if j != i && new_session {
                sessions.push(Session {
                    person,
                    start: session_start,
                    day: session_day,
                    revenue: session_revenue,
                    has_purchase: session_purchase,
                });
                session_start = ts;
                session_day = day(ts);
                session_revenue = 0;
                session_purchase = false;
            }
            let e = surviving[j];
            let agg = users.entry((person, session_day)).or_default();
            agg.events += 1;
            if e.kind == 2 {
                let amount = e.price.unwrap();
                session_revenue += amount;
                session_purchase = true;
                agg.revenue += amount;
                let product = e.product.unwrap();
                let category = *products.get(&product).unwrap_or(&unknown);
                *agg.categories.entry(category).or_default() += amount;
            }
            if new_session {
                users.entry((person, session_day)).or_default().sessions += 1;
            }
            previous_ts = ts;
            j += 1;
        }
        sessions.push(Session {
            person,
            start: session_start,
            day: session_day,
            revenue: session_revenue,
            has_purchase: session_purchase,
        });
        i = j;
    }

    eprintln!(
        "sessionized and aggregated users in {:.3}s",
        phase.elapsed().as_secs_f64()
    );
    let phase = Instant::now();
    let mut click_rank = raw_clicks
        .iter()
        .enumerate()
        .map(|(index, click)| (click.id, index))
        .collect::<Vec<_>>();
    click_rank.sort_unstable_by(|a, b| a.0.bytes().cmp(b.0.bytes()));
    let mut ranks = vec![0u32; raw_clicks.len()];
    for (rank, (_, index)) in click_rank.into_iter().enumerate() {
        ranks[index] = rank as u32;
    }
    let clicks = raw_clicks
        .into_iter()
        .zip(ranks)
        .map(|(click, id)| Click {
            id,
            user: click.user,
            ts: click.ts,
            campaign: click.campaign,
        })
        .collect::<Vec<_>>();
    drop(click_sources);
    let mut campaign_daily: FastMap<(Id, i32), CampaignAgg> = fast_map();
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
        let person_clicks = &clicks[ci..ce];
        let mut active: BTreeSet<(i64, Reverse<u32>, Id)> = BTreeSet::new();
        let mut campaign_counts: FastMap<Id, u32> = fast_map();
        let (mut add, mut remove) = (0usize, 0usize);
        for s in &sessions[si..se] {
            while add < person_clicks.len() && person_clicks[add].ts <= s.start {
                let c = person_clicks[add];
                active.insert((c.ts, Reverse(c.id), c.campaign));
                *campaign_counts.entry(c.campaign).or_default() += 1;
                add += 1;
            }
            let cutoff = s.start.saturating_sub(ATTRIBUTION_MS);
            while remove < add && person_clicks[remove].ts < cutoff {
                let c = person_clicks[remove];
                active.remove(&(c.ts, Reverse(c.id), c.campaign));
                if let Some(n) = campaign_counts.get_mut(&c.campaign) {
                    *n -= 1;
                    if *n == 0 {
                        campaign_counts.remove(&c.campaign);
                    }
                }
                remove += 1;
            }
            let campaign = active.iter().next_back().map(|x| x.2).unwrap_or(organic);
            let agg = campaign_daily.entry((campaign, s.day)).or_default();
            agg.sessions += 1;
            agg.revenue += s.revenue;
            agg.purchasing_sessions += i64::from(s.has_purchase);
            for &assisted in campaign_counts.keys() {
                campaign_daily
                    .entry((assisted, s.day))
                    .or_default()
                    .assisted_sessions += 1;
            }
        }
        ci = ce;
        si = se;
    }

    let user_totals = users
        .values()
        .fold((0i64, 0i64), |(sessions, revenue), row| {
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
    users: &FastMap<(Id, i32), UserAgg>,
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
    for (&(user, d), agg) in user_rows {
        user_ids.push(strings.get(user));
        user_days.push(d);
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
        vec![
            Arc::new(StringArray::from(user_ids)),
            Arc::new(Date32Array::from(user_days)),
            Arc::new(Int64Array::from(user_sessions)),
            Arc::new(Int64Array::from(user_events)),
            Arc::new(Int64Array::from(user_revenue)),
            Arc::new(StringArray::from(top_category)),
        ],
    )?;

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
        vec![
            Arc::new(StringArray::from(campaign_ids)),
            Arc::new(StringArray::from(channels_out)),
            Arc::new(Date32Array::from(campaign_days)),
            Arc::new(Int64Array::from(campaign_sessions)),
            Arc::new(Int64Array::from(purchasing)),
            Arc::new(Int64Array::from(revenue)),
            Arc::new(Int64Array::from(assists)),
        ],
    )?;
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("shopping-sessions: {error}");
        std::process::exit(1);
    }
}
