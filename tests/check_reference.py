#!/usr/bin/env python3
"""Cross-check a generated small dataset against the independent Python oracle."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys
import tempfile

import polars as pl

from reference import compute

SCHEMAS = {
    "user_daily": {
        "user_id": pl.String,
        "day": pl.Date,
        "sessions": pl.Int64,
        "events": pl.Int64,
        "revenue_cents": pl.Int64,
        "top_category": pl.String,
    },
    "campaign_daily": {
        "campaign_id": pl.String,
        "channel": pl.String,
        "day": pl.Date,
        "sessions": pl.Int64,
        "purchasing_sessions": pl.Int64,
        "revenue_cents": pl.Int64,
        "assisted_sessions": pl.Int64,
    },
}


def sort_rows(values):
    return sorted(values, key=lambda row: tuple("" if value is None else str(value) for value in row))


def main() -> int:
    parser = argparse.ArgumentParser()
    default_binary = "target/release/shopping-sessions.exe" if os.name == "nt" else "target/release/shopping-sessions"
    parser.add_argument("--binary", default=default_binary)
    parser.add_argument("--data", required=True)
    args = parser.parse_args()
    binary, data = Path(args.binary).resolve(), Path(args.data).resolve()
    expected = compute(data)
    with tempfile.TemporaryDirectory(prefix="shopping-sessions-reference-") as temp:
        output = Path(temp) / "out"
        output.mkdir()
        env = dict(os.environ, DATA_DIR=str(data), OUTPUT_DIR=str(output), SCRATCH_DIR=temp, NCPU="8")
        result = subprocess.run([binary], env=env, capture_output=True, text=True)
        if result.returncode:
            print(result.stderr)
            return result.returncode
        ok = True
        for table, schema in SCHEMAS.items():
            actual = pl.concat([pl.read_parquet(path) for path in (output / table).glob("*.parquet")])
            actual_rows = actual.select(schema.keys()).cast(schema).rows()
            if sort_rows(expected[table]) != sort_rows(actual_rows):
                print(f"FAIL {table}: oracle={len(expected[table])} rows, Rust={actual.height} rows")
                ok = False
            else:
                print(f"ok {table}: {actual.height} rows exact")
        if not ok:
            print(result.stderr)
    return int(not ok)


if __name__ == "__main__":
    raise SystemExit(main())
