#!/usr/bin/env python3
"""Measure full-process wall time on a generated dataset."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import statistics
import subprocess
import sys
import tempfile
import time

from runner import command


def main() -> int:
    parser = argparse.ArgumentParser()
    default_binary = "target/release/shopping-sessions.exe" if os.name == "nt" else "target/release/shopping-sessions"
    parser.add_argument("--binary", default=default_binary)
    parser.add_argument("--docker-image")
    parser.add_argument("--data", required=True)
    parser.add_argument("--runs", type=int, default=3)
    args = parser.parse_args()
    binary, data = Path(args.binary).resolve(), Path(args.data).resolve()
    times = []
    for index in range(args.runs):
        with tempfile.TemporaryDirectory(prefix="shopping-sessions-bench-") as temp:
            output = Path(temp) / "out"
            scratch = Path(temp) / "scratch"
            output.mkdir()
            scratch.mkdir()
            if args.docker_image:
                cmd = command(binary, args.docker_image, data, output, scratch)
                env = os.environ.copy()
            else:
                cmd = command(binary, None, data, output, scratch)
                env = dict(os.environ, DATA_DIR=str(data), OUTPUT_DIR=str(output), SCRATCH_DIR=str(scratch), NCPU="8")
            start = time.perf_counter()
            result = subprocess.run(cmd, env=env, capture_output=True, text=True)
            elapsed = time.perf_counter() - start
            if result.returncode:
                print(f"run failed with exit code {result.returncode}", file=sys.stderr)
                print(result.stderr)
                return result.returncode
            times.append(elapsed)
            print(f"run {index + 1}: {elapsed:.3f}s ({result.stderr.strip()})")
    print(f"median: {statistics.median(times):.3f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
