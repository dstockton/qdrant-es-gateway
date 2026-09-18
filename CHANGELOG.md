# Changelog

## Unreleased

- Add GitHub Pages documentation with reviewed architecture, operations, lifecycle, pattern-query, and pagination articles.
- Add `_msearch`, `search_after` cursors with returned sort values, and common `_refresh`, `_open`, `_close`, and `_settings` lifecycle request compatibility.
- Extend the endpoint-only validation replay and document request-level compatibility for the AWS Retail Demo Store and Spinscale catalogue app.

## 0.1.0

Initial vertical slice: Rust Axum gateway, Qdrant server-side BM25, durable mappings and aliases, deterministic ES IDs, CRUD, bulk, search/filter/count, health endpoints, Docker Compose quickstart, structured compatibility errors, and local compatibility analytics.
