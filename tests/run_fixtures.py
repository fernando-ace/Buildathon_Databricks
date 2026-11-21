#!/usr/bin/env python3
"""Run every official edge-case fixture against the native executable."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import tempfile

import polars as pl
from runner import command

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


def normalized(frame: pl.DataFrame, schema: dict[str, pl.DataType]) -> list[tuple]:
    if set(frame.columns) != set(schema):
        raise ValueError(f"columns differ: expected {list(schema)}, got {frame.columns}")
    for column, expected in schema.items():
        actual = frame.schema[column]
        if expected == pl.Int64 and actual.is_integer():
            continue
        if actual != expected:
            raise ValueError(f"{column} has type {actual}; expected {expected}")
    return sorted(frame.select(schema.keys()).cast(schema).rows(), key=lambda row: tuple("" if v is None else str(v) for v in row))


def main() -> int:
    parser = argparse.ArgumentParser()
    default_binary = "target/release/shopping-sessions.exe" if os.name == "nt" else "target/release/shopping-sessions"
    parser.add_argument("--binary", default=default_binary)
    parser.add_argument("--docker-image")
    parser.add_argument("--fixtures", default="tests/fixtures")
    args = parser.parse_args()
    binary = Path(args.binary).resolve()
    fixture_root = Path(args.fixtures)
    cases = sorted(path for path in fixture_root.iterdir() if path.is_dir())
    failed = []
    for case in cases:
        data = case / "input"
        if not data.exists():
            data = case / "data"
        with tempfile.TemporaryDirectory(prefix="shopping-sessions-") as temp:
            output = Path(temp) / "out"
            scratch = Path(temp) / "scratch"
            output.mkdir()
            scratch.mkdir()
            if args.docker_image:
                cmd = command(binary, args.docker_image, data, output, scratch)
                env = os.environ.copy()
            else:
                cmd = command(binary, None, data, output, scratch)
                env = dict(os.environ, DATA_DIR=str(data.resolve()), OUTPUT_DIR=str(output), SCRATCH_DIR=str(scratch))
            result = subprocess.run(cmd, env=env, capture_output=True, text=True)
            if result.returncode:
                failed.append(case.name)
                print(f"FAIL {case.name}: exit {result.returncode}\n{result.stderr[-1500:]}")
                continue
            ok = True
            for table, schema in SCHEMAS.items():
                expected = pl.read_csv(
                    case / "expected" / f"{table}.csv",
                    schema_overrides={
                        name: (pl.String if dtype == pl.Date else pl.Int64 if dtype == pl.Int64 else dtype)
                        for name, dtype in schema.items()
                    },
                ).with_columns(pl.col("day").str.to_date())
                files = sorted((output / table).glob("*.parquet"))
                if not files:
                    print(f"FAIL {case.name}/{table}: no parquet output")
                    ok = False
                    continue
                actual = pl.concat([pl.read_parquet(path) for path in files])
                try:
                    if normalized(expected, schema) != normalized(actual, schema):
                        print(f"FAIL {case.name}/{table}: rows differ\n expected={normalized(expected, schema)}\n actual={normalized(actual, schema)}")
                        ok = False
                except ValueError as error:
                    print(f"FAIL {case.name}/{table}: {error}")
                    ok = False
            if ok:
                print(f"ok {case.name}")
            else:
                failed.append(case.name)
    print(f"\n{len(cases) - len(failed)}/{len(cases)} fixtures passed")
    return int(bool(failed))


if __name__ == "__main__":
    raise SystemExit(main())
