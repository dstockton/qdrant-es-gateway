FROM rust:1.88-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && apt-get upgrade -y \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --uid 10001 gateway
WORKDIR /app
COPY --from=builder /app/target/release/qdrant-es-gateway /usr/local/bin/qdrant-es-gateway
ENV LISTEN_ADDR=0.0.0.0:9200 QDRANT_URL=http://qdrant:6333 METADATA_DB=/data/gateway.db
RUN mkdir -p /data && chown -R 10001:10001 /data /app
USER 10001:10001
EXPOSE 9200
VOLUME ["/data"]
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 CMD ["sh", "-c", "kill -0 1"]
ENTRYPOINT ["qdrant-es-gateway"]
