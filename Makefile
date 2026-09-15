.PHONY: dev test lint integration-test benchmark clean

dev:
	cargo run

test:
	cargo test

lint:
	cargo fmt -- --check
	cargo clippy --all-targets --all-features -- -D warnings

integration-test:
	docker compose up -d qdrant
	QDRANT_URL=http://localhost:6333 cargo test --test integration -- --ignored --nocapture

benchmark:
	@echo "Benchmark harness is intentionally reproducible via scripts/benchmark.sh; run docker compose up -d first."
	@./scripts/benchmark.sh

clean:
	cargo clean
