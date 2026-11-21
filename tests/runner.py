"""Construct native or scorer-like commands for test runs."""
from __future__ import annotations

from pathlib import Path


def command(binary: Path, image: str | None, data: Path, output: Path, scratch: Path):
    if image is None:
        return [str(binary)]
    return [
        "docker", "run", "--rm", "--read-only", "--cpus=8", "--memory=24g",
        "--tmpfs", "/tmp:rw,size=64m",
        "--env", "DATA_DIR=/data", "--env", "OUTPUT_DIR=/output",
        "--env", "SCRATCH_DIR=/scratch", "--env", "HOME=/scratch", "--env", "NCPU=8", "--env", "MEM_GB=24",
        "--volume", f"{data.resolve()}:/data:ro",
        "--volume", f"{output.resolve()}:/output:rw",
        "--volume", f"{scratch.resolve()}:/scratch:rw",
        image,
    ]
