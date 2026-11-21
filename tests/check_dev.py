#!/usr/bin/env python3
"""Compare the public dev run with its supplied exact answer."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import tempfile

import polars as pl

TABLES = {
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


def rows(frame: pl.DataFrame, schema: dict[str, pl.DataType]) -> list[tuple]:
    return sorted(
        frame.select(schema.keys()).cast(schema).rows(),
        key=lambda row: tuple("" if value is None else str(value) for value in row),
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    default_binary = "target/release/shopping-sessions.exe" if os.name == "nt" else "target/release/shopping-sessions"
    parser.add_argument("--binary", default=default_binary)
    parser.add_argument("--data", default="dev")
    parser.add_argument("--answer", default="tests/dev-answer")
    args = parser.parse_args()
    binary, data, answer = Path(args.binary).resolve(), Path(args.data).resolve(), Path(args.answer)
    with tempfile.TemporaryDirectory(prefix="shopping-sessions-dev-") as temp:
        output = Path(temp) / "out"
        output.mkdir()
        env = dict(os.environ, DATA_DIR=str(data), OUTPUT_DIR=str(output), SCRATCH_DIR=temp)
        result = subprocess.run([binary], env=env, capture_output=True, text=True)
        if result.returncode:
            print(result.stderr)
            return result.returncode
        ok = True
        for table, schema in TABLES.items():
            expected = pl.read_csv(
                answer / f"{table}.csv",
                schema_overrides={name: pl.String if dtype == pl.Date else dtype for name, dtype in schema.items()},
            ).with_columns(pl.col("day").str.to_date())
            actual_files = sorted((output / table).glob("*.parquet"))
            actual = pl.concat([pl.read_parquet(path) for path in actual_files])
            if rows(expected, schema) != rows(actual, schema):
                print(f"FAIL {table}: expected {expected.height} rows, got {actual.height}")
                ok = False
            else:
                print(f"ok {table}: {actual.height} rows exact")
        if not ok:
            print(result.stderr)
    return int(not ok)


if __name__ == "__main__":
    raise SystemExit(main())
