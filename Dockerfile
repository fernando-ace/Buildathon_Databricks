FROM rust:1.100-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app \
    && mkdir -p /app /scratch \
    && chown 1000:1000 /scratch
COPY --from=build /src/target/release/shopping-sessions /app/shopping-sessions
USER 1000:1000
WORKDIR /app
ENV DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch HOME=/scratch
ENTRYPOINT ["/app/shopping-sessions"]

