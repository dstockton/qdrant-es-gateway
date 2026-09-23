# Changelog

## Unreleased

- Return Elasticsearch-shaped HTTP 413 errors for requests that exceed `MAX_BODY_BYTES` or `MAX_BULK_BYTES`, including bodies rejected while streaming.
- Default the Helm deployment to one replica so its SQLite metadata catalog and `ReadWriteOnce` volume are not presented as a safe horizontally scaled control plane.
- Publish index metadata only after Qdrant setup succeeds, and clean up collections after partial creation failures.
- Decode percent-encoded route segments so document IDs containing slashes, spaces, or Unicode retain their Elasticsearch identity.
- Add GitHub Pages documentation with reviewed architecture, operations, lifecycle, pattern-query, and pagination articles.
- Add `_msearch`, `search_after` cursors with returned sort values, and common `_refresh`, `_open`, `_close`, and `_settings` lifecycle request compatibility.
- Extend the endpoint-only validation replay and document request-level compatibility for the AWS Retail Demo Store and Spinscale catalogue app.
- Add a reproducible external-corpus benchmark path and a local importer for the public H&M product catalogue.
- Document additional Elasticsearch-backed open-source application targets and larger public dataset options.

## 0.1.0

Initial vertical slice: Rust Axum gateway, Qdrant server-side BM25, durable mappings and aliases, deterministic ES IDs, CRUD, bulk, search/filter/count, health endpoints, Docker Compose quickstart, structured compatibility errors, and local compatibility analytics.
