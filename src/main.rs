use ahash::RandomState;
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
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

type Id = u32;
type FastMap<K, V> = HashMap<K, V, RandomState>;
type FastSet<K> = HashSet<K, RandomState>;
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
    if bytes.is_empty() || bytes.iter().any(|b| !b.is_ascii_digit()) {
        return None;
    }
    bytes.iter().try_fold(0u64, |n, b| {
        n.checked_mul(10)?.checked_add((b - b'0') as u64)
    })
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
    if bytes.iter().all(u8::is_ascii_digit) {
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

fn each_row<const N: usize, F>(path: &Path, mut f: F) -> Result<(), String>
where
    F: FnMut([&[u8]; N]),
{
    let bytes = fs::read(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    let mut lines = bytes.split(|b| *b == b'\n');
    let _header = lines.next();
    for line in lines {
        if let Some(row) = fields::<N>(line) {
            f(row);
        }
    }
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

fn parse_event_file(path: &Path) -> Result<(Interner, FastMap<String, Event>), String> {
    let mut strings = Interner::default();
    let mut events: FastMap<String, Event> = fast_map();
    each_row::<7, _>(path, |row| {
        let (
            Some(event_id),
            Some(user),
            Some(ts),
            Some(ingest),
            Some(kind),
            Some(product),
            Some(price_raw),
        ) = (
            text(row[0]),
            text(row[1]),
            text(row[2]),
            text(row[3]),
            text(row[4]),
            text(row[5]),
            text(row[6]),
        )
        else {
            return;
        };
        if event_id.is_empty() {
            return;
        }
        let kind = match kind.as_bytes() {
            b"view" => 0,
            b"cart" => 1,
            b"purchase" => 2,
            _ => 255,
        };
        let event = Event {
            user: (!user.is_empty()).then(|| strings.intern(user)),
            ts: timestamp(ts.as_bytes()),
            kind,
            product: (!product.is_empty()).then(|| strings.intern(product)),
            price: (kind == 2).then(|| price(price_raw.as_bytes())).flatten(),
            ingest: timestamp(ingest.as_bytes()).unwrap_or(i64::MAX),
        };
        match events.entry(event_id.to_owned()) {
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(event);
            }
            std::collections::hash_map::Entry::Occupied(mut e) => {
                if event.ingest < e.get().ingest {
                    e.insert(event);
                }
            }
        }
    })?;
    Ok((strings, events))
}

fn parse_events(data: &Path, strings: &mut Interner, ncpu: usize) -> Result<Vec<Event>, String> {
    let paths = csv_files(&data.join("events"))?;
    let mut merged: FastMap<String, Event> = fast_map();
    for batch in paths.chunks(ncpu) {
        let parsed = batch
            .par_iter()
            .map(|path| parse_event_file(path))
            .collect::<Result<Vec<_>, _>>()?;
        for (local_strings, mut local_events) in parsed {
            let remap = local_strings
                .values
                .iter()
                .map(|value| strings.intern(value))
                .collect::<Vec<_>>();
            for (event_id, mut event) in local_events.drain() {
                event.user = event.user.map(|id| remap[id as usize]);
                event.product = event.product.map(|id| remap[id as usize]);
                match merged.entry(event_id) {
                    std::collections::hash_map::Entry::Vacant(e) => {
                        e.insert(event);
                    }
                    std::collections::hash_map::Entry::Occupied(mut e) => {
                        if event.ingest < e.get().ingest {
                            e.insert(event);
                        }
                    }
                }
            }
        }
    }
    Ok(merged.into_values().collect())
}

fn parse_click_file(path: &Path) -> Result<(Interner, Vec<Click>), String> {
    let mut strings = Interner::default();
    let mut seen: FastSet<String> = fast_set();
    let mut clicks = Vec::new();
    each_row::<4, _>(path, |row| {
        let (Some(id), Some(user), Some(campaign), Some(ts)) =
            (text(row[0]), text(row[1]), text(row[2]), text(row[3]))
        else {
            return;
        };
        if id.is_empty() || user.is_empty() || !seen.insert(id.to_owned()) {
            return;
        }
        if let Some(ts) = timestamp(ts.as_bytes()) {
            clicks.push(Click {
                id: strings.intern(id),
                user: strings.intern(user),
                ts,
                campaign: strings.intern(campaign),
            });
        }
    })?;
    Ok((strings, clicks))
}

fn parse_clicks(data: &Path, strings: &mut Interner, ncpu: usize) -> Result<Vec<Click>, String> {
    let paths = csv_files(&data.join("clicks"))?;
    let mut seen: FastSet<Id> = fast_set();
    let mut clicks = Vec::new();
    for batch in paths.chunks(ncpu) {
        let parsed = batch
            .par_iter()
            .map(|path| parse_click_file(path))
            .collect::<Result<Vec<_>, _>>()?;
        for (local_strings, local_clicks) in parsed {
            let remap = local_strings
                .values
                .iter()
                .map(|value| strings.intern(value))
                .collect::<Vec<_>>();
            for mut click in local_clicks {
                click.id = remap[click.id as usize];
                click.user = remap[click.user as usize];
                click.campaign = remap[click.campaign as usize];
                if seen.insert(click.id) {
                    clicks.push(click);
                }
            }
        }
    }
    Ok(clicks)
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
    let mut clicks = parse_clicks(&data, &mut strings, ncpu)?;
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
    for c in &mut clicks {
        if c.ts < start.saturating_sub(ATTRIBUTION_MS)
            || c.ts >= end
            || !campaigns.contains_key(&c.campaign)
        {
            c.ts = i64::MIN;
        } else {
            c.user = dsu.canonical(c.user);
        }
    }
    clicks.retain(|c| c.ts != i64::MIN);
    eprintln!(
        "linked IDs and filtered records in {:.3}s",
        phase.elapsed().as_secs_f64()
    );

    let phase = Instant::now();
    surviving.par_sort_unstable_by_key(|e| (e.user.unwrap(), e.ts.unwrap()));
    clicks.par_sort_unstable_by_key(|c| (c.user, c.ts));
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

    let mut click_rank: Vec<(Id, Id)> = clicks.iter().map(|c| (c.id, c.id)).collect();
    eprintln!(
        "sessionized and aggregated users in {:.3}s",
        phase.elapsed().as_secs_f64()
    );
    let phase = Instant::now();
    click_rank
        .sort_unstable_by(|a, b| strings.get(a.0).as_bytes().cmp(strings.get(b.0).as_bytes()));
    let mut ranks = vec![0u32; strings.values.len()];
    for (rank, (id, _)) in click_rank.into_iter().enumerate() {
        ranks[id as usize] = rank as u32;
    }
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
                active.insert((c.ts, Reverse(ranks[c.id as usize]), c.campaign));
                *campaign_counts.entry(c.campaign).or_default() += 1;
                add += 1;
            }
            let cutoff = s.start.saturating_sub(ATTRIBUTION_MS);
            while remove < add && person_clicks[remove].ts < cutoff {
                let c = person_clicks[remove];
                active.remove(&(c.ts, Reverse(ranks[c.id as usize]), c.campaign));
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
