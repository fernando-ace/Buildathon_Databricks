FROM rust:1.100-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
ARG TARGET_CPU=x86-64-v3
ENV RUSTFLAGS="-C target-cpu=${TARGET_CPU} -C target-feature=+crt-static"
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked --target x86_64-unknown-linux-gnu \
    && cp /src/target/x86_64-unknown-linux-gnu/release/shopping-sessions /shopping-sessions

FROM scratch
COPY --from=build /shopping-sessions /shopping-sessions
USER 1000:1000
ENV DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch HOME=/scratch
ENTRYPOINT ["/shopping-sessions"]

