#!/usr/bin/env python3
"""Measure full-process wall time on a generated dataset."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import statistics
import subprocess
import tempfile
import time


def main() -> int:
    parser = argparse.ArgumentParser()
    default_binary = "target/release/shopping-sessions.exe" if os.name == "nt" else "target/release/shopping-sessions"
    parser.add_argument("--binary", default=default_binary)
    parser.add_argument("--data", required=True)
    parser.add_argument("--runs", type=int, default=3)
    args = parser.parse_args()
    binary, data = Path(args.binary).resolve(), Path(args.data).resolve()
    times = []
    for index in range(args.runs):
        with tempfile.TemporaryDirectory(prefix="shopping-sessions-bench-") as temp:
            output = Path(temp) / "out"
            output.mkdir()
            env = dict(os.environ, DATA_DIR=str(data), OUTPUT_DIR=str(output), SCRATCH_DIR=temp, NCPU="8")
            start = time.perf_counter()
            result = subprocess.run([binary], env=env, capture_output=True, text=True)
            elapsed = time.perf_counter() - start
            if result.returncode:
                print(result.stderr)
                return result.returncode
            times.append(elapsed)
            print(f"run {index + 1}: {elapsed:.3f}s ({result.stderr.strip()})")
    print(f"median: {statistics.median(times):.3f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
