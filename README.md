# Shopping Sessions

A native Rust implementation of the Builders / Databricks Shopping Sessions
track. It reads the CSV files from `DATA_DIR` and writes the two required
Parquet tables under `OUTPUT_DIR`.

## Build and run

```sh
docker buildx build --platform linux/amd64 --load -t shopping-sessions .
docker run --rm --cpus=8 --memory=24g --read-only --tmpfs /tmp:rw,size=64m \
  -v ./data:/data:ro -v ./output:/output:rw -v ./scratch:/scratch:rw \
  shopping-sessions
```

The image has no runtime network dependency. At runtime, data is read-only and
temporary state is restricted to `/scratch` and `/tmp`.

## Correctness checks

Install the test reader with `python -m pip install -r requirements-test.txt`.
The public 36 fixtures are in `tests/fixtures`; run `make fixtures` to execute
the native release binary against all of them. Run `make container-fixtures`
to exercise the Docker image with scorer-like limits. Generate the public dev
input with the track's `generate` utility, then run `make dev-check` to compare
it with the supplied answer (the generated `dev/` input is not committed).
For a generated small dataset, `python tests/check_reference.py --data PATH`
cross-checks both output tables against a simple Python oracle. `python
tests/benchmark.py --data PATH` measures full-process wall time; add
`--docker-image shopping-sessions:latest` to benchmark in the container.

Rules and scorer contract: <https://www.joinbuilders.org/buildathon/tracks/shopping-sessions>.

