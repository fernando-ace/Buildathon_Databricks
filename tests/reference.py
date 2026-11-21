"""Small, deliberately direct implementation of the published rules."""
from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import re

DAY_MS = 86_400_000
GAP_MS = 1_800_000
ATTRIBUTION_MS = 604_800_000


def rows(path: Path, columns: int):
    for line in path.read_bytes().split(b"\n")[1:]:
        if not line:
            continue
        raw = line.split(b",")
        if len(raw) != columns:
            continue
        fields = []
        valid = True
        for value in raw:
            if value.startswith(b'"'):
                if len(value) < 2 or not value.endswith(b'"') or b'"' in value[1:-1]:
                    valid = False
                    break
                value = value[1:-1]
            elif b'"' in value:
                valid = False
                break
            try:
                fields.append(value.decode("utf-8"))
            except UnicodeDecodeError:
                valid = False
                break
        if valid:
            yield fields


def timestamp(value: str):
    if value.isascii() and value.isdigit():
        number = int(value)
        return number if number <= (1 << 63) - 1 else None
    if re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z", value):
        try:
            fmt = "%Y-%m-%dT%H:%M:%SZ"
            parsed = datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
            return int(parsed.timestamp() * 1000)
        except ValueError:
            return None
    if re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+-][0-9]{2}:[0-9]{2}", value):
        try:
            parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S%z")
            return int(parsed.timestamp() * 1000)
        except ValueError:
            return None
    return None


def price(value: str):
    if not value or not value.isascii() or not value.isdigit():
        return None
    number = int(value)
    return number if 1 <= number <= 10_000_000 else None


class UnionFind:
    def __init__(self):
        self.parent = {}
        self.minimum = {}

    def add(self, value):
        if value not in self.parent:
            self.parent[value] = value
            self.minimum[value] = value

    def find(self, value):
        self.add(value)
        if self.parent[value] != value:
            self.parent[value] = self.find(self.parent[value])
        return self.parent[value]

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self.minimum[ra].encode() > self.minimum[rb].encode():
            ra, rb = rb, ra
        self.parent[rb] = ra
        if self.minimum[rb].encode() < self.minimum[ra].encode():
            self.minimum[ra] = self.minimum[rb]

    def canonical(self, value):
        root = self.find(value)
        return self.minimum[root]


def day(ts: int) -> date:
    return date(1970, 1, 1) + timedelta(days=ts // DAY_MS)


def compute(data: Path):
    manifest = __import__("json").loads((data / "manifest.json").read_text(encoding="utf-8"))
    window_start = timestamp(manifest["window_start"])
    window_end = timestamp(manifest["window_end"])
    products = {r[0]: r[1] for r in rows(data / "products.csv", 2)}
    campaigns = {r[0]: r[1] for r in rows(data / "campaigns.csv", 2)}

    uf = UnionFind()
    links = []
    for path in sorted((data / "links").glob("*.csv")):
        for a, b in rows(path, 2):
            if a and b:
                links.append((a, b))
    for a, b in links:
        uf.union(a, b)

    event_by_id = {}
    for path in sorted((data / "events").glob("*.csv")):
        for record in rows(path, 7):
            event_id, user, ts_raw, ingest_raw, kind, product, amount_raw = record
            ingest = timestamp(ingest_raw)
            if not event_id or ingest is None:
                continue
            candidate = (user, timestamp(ts_raw), kind, product, price(amount_raw) if kind == "purchase" else None, ingest)
            if event_id not in event_by_id or ingest < event_by_id[event_id][-1]:
                event_by_id[event_id] = candidate

    by_person = defaultdict(list)
    for user, ts, kind, product, amount, ingest in event_by_id.values():
        if not user or ts is None or not window_start <= ts < window_end:
            continue
        if kind not in ("view", "cart", "purchase") or not product:
            continue
        if kind == "purchase" and amount is None:
            continue
        if ingest - ts > DAY_MS:
            continue
        by_person[uf.canonical(user)].append((ts, kind, product, amount))

    user_daily = defaultdict(lambda: {"sessions": 0, "events": 0, "revenue_cents": 0, "categories": defaultdict(int)})
    sessions = []
    for person, events in by_person.items():
        events.sort(key=lambda e: e[0])
        current = []
        previous_ts = None
        for event in events:
            ts = event[0]
            if current and ts - previous_ts > GAP_MS:
                sessions.append((person, current))
                current = []
            current.append(event)
            previous_ts = ts
        if current:
            sessions.append((person, current))

    campaign_clicks = defaultdict(list)
    click_by_id = {}
    for path in sorted((data / "clicks").glob("*.csv")):
        for click_id, user, campaign, ts_raw in rows(path, 4):
            if click_id and click_id not in click_by_id:
                click_by_id[click_id] = (user, campaign, timestamp(ts_raw))
    for click_id, (user, campaign, ts) in click_by_id.items():
        if not user or ts is None or campaign not in campaigns:
            continue
        if not window_start - ATTRIBUTION_MS <= ts < window_end:
            continue
        campaign_clicks[uf.canonical(user)].append((ts, click_id, campaign))
    for clicks in campaign_clicks.values():
        clicks.sort()

    campaign_daily = defaultdict(lambda: {"sessions": 0, "purchasing_sessions": 0, "revenue_cents": 0, "assisted_sessions": 0})
    for person, events in sessions:
        start_ts = events[0][0]
        session_day = day(start_ts)
        revenue = sum(e[3] or 0 for e in events if e[1] == "purchase")
        has_purchase = any(e[1] == "purchase" for e in events)
        user_key = (person, session_day)
        user_daily[user_key]["sessions"] += 1
        user_daily[user_key]["events"] += len(events)
        user_daily[user_key]["revenue_cents"] += revenue
        for _, kind, product, amount in events:
            if kind == "purchase":
                user_daily[user_key]["categories"][products.get(product, "unknown")] += amount

        eligible = [c for c in campaign_clicks.get(person, []) if start_ts - ATTRIBUTION_MS <= c[0] <= start_ts]
        credited = min(eligible, key=lambda c: (-c[0], c[1].encode()))[2] if eligible else "organic"
        credited_row = campaign_daily[(credited, session_day)]
        credited_row["sessions"] += 1
        credited_row["purchasing_sessions"] += int(has_purchase)
        credited_row["revenue_cents"] += revenue
        for assisted in {c[2] for c in eligible}:
            campaign_daily[(assisted, session_day)]["assisted_sessions"] += 1

    user_rows = []
    for (person, session_day), aggregate in user_daily.items():
        categories = aggregate["categories"]
        top = min(categories, key=lambda cat: (-categories[cat], cat.encode())) if categories else None
        user_rows.append((person, session_day, aggregate["sessions"], aggregate["events"], aggregate["revenue_cents"], top))
    campaign_rows = [
        (campaign, "organic" if campaign == "organic" else campaigns[campaign], session_day, values["sessions"], values["purchasing_sessions"], values["revenue_cents"], values["assisted_sessions"])
        for (campaign, session_day), values in campaign_daily.items()
    ]
    return {
        "user_daily": sorted(user_rows),
        "campaign_daily": sorted(campaign_rows),
    }
