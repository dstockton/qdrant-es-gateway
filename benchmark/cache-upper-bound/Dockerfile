FROM rust:1.88-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock main.rs hit.lua ./
RUN cargo build --release --locked
FROM debian:bookworm-slim
COPY --from=builder /app/target/release/qeg-cache-upper-bound /usr/local/bin/qeg-cache-upper-bound
USER 10001:10001
ENTRYPOINT ["qeg-cache-upper-bound"]
