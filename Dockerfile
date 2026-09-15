FROM rust:latest AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/qdrant-es-gateway /usr/local/bin/qdrant-es-gateway
ENV LISTEN_ADDR=0.0.0.0:9200 QDRANT_URL=http://qdrant:6333 METADATA_DB=/data/gateway.db
RUN mkdir -p /data
EXPOSE 9200
VOLUME ["/data"]
ENTRYPOINT ["qdrant-es-gateway"]
