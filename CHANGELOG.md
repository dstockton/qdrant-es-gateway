# Changelog

## [0.1.3](https://github.com/dstockton/qdrant-es-gateway/compare/qdrant-es-gateway-v0.1.2...qdrant-es-gateway-v0.1.3) (2026-09-25)


### Bug Fixes

* grant release workflow publish permissions ([34aeb70](https://github.com/dstockton/qdrant-es-gateway/commit/34aeb70cfb5d9b6e76a5c31908989c4190bad6fe))
* make release workflow dependency explicit ([ce13c14](https://github.com/dstockton/qdrant-es-gateway/commit/ce13c149215f0f908d4e11e034715bbe510b8d1a))
* merge Dependabot updates without branch protection ([da05dc6](https://github.com/dstockton/qdrant-es-gateway/commit/da05dc6a16afe9387c2823857f792a2c51a4ccaf))
* pin Release Please to a commit ([0837fb9](https://github.com/dstockton/qdrant-es-gateway/commit/0837fb97db0099246b6a0ad396d8d16af508ce6a))

## 0.1.2 - 2026-09-24

- Added configurable upstream timeouts.
- Bounded asynchronous payload writes.
- Added SQLite busy handling and dependency advisory checks.

## Unreleased

- Reject index names that would collide in Qdrant's normalized search/document collection namespace before creating upstream state.
- Return HTTP 404 with `result: "not_found"` when deleting a missing document, including non-error bulk item reporting.
- Return `document_missing_exception` for updates to missing documents while preserving explicit `doc_as_upsert` creation, including correct bulk error and status reporting.
- Return HTTP 404 with Elasticsearch's `found: false` document shape when a GET targets a missing document.
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
