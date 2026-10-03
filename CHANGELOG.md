# Changelog

## [0.2.0](https://github.com/dstockton/qdrant-es-gateway/compare/v0.1.3...v0.2.0) (2026-10-03)


### Features

* support explicit update upserts ([#17](https://github.com/dstockton/qdrant-es-gateway/issues/17)) ([116c8ed](https://github.com/dstockton/qdrant-es-gateway/commit/116c8ede79692b592dacb1d02b2575aa42d8304a))
* support standalone create API ([bd7bb07](https://github.com/dstockton/qdrant-es-gateway/commit/bd7bb07b5439111b4687c4764f3148ff11c52145))
* support standalone create API ([b3eb361](https://github.com/dstockton/qdrant-es-gateway/commit/b3eb361b655fa08528b6149b951a4a0280bfabf4))


### Bug Fixes

* apply alias actions atomically ([092d011](https://github.com/dstockton/qdrant-es-gateway/commit/092d011e9417fb8c42887c4ee55fd8a908bed44b))
* correct exists query semantics ([e291d03](https://github.com/dstockton/qdrant-es-gateway/commit/e291d03996f1085a3348c6595d4d1c32c08f8f34))
* grant release workflow publish permissions ([34aeb70](https://github.com/dstockton/qdrant-es-gateway/commit/34aeb70cfb5d9b6e76a5c31908989c4190bad6fe))
* keep release tags unprefixed ([#8](https://github.com/dstockton/qdrant-es-gateway/issues/8)) ([65bb846](https://github.com/dstockton/qdrant-es-gateway/commit/65bb8464bb2e554e66d381dba0c92e5fbdb62ab0))
* make release workflow dependency explicit ([ce13c14](https://github.com/dstockton/qdrant-es-gateway/commit/ce13c149215f0f908d4e11e034715bbe510b8d1a))
* merge Dependabot updates without branch protection ([da05dc6](https://github.com/dstockton/qdrant-es-gateway/commit/da05dc6a16afe9387c2823857f792a2c51a4ccaf))
* pin Release Please to a commit ([0837fb9](https://github.com/dstockton/qdrant-es-gateway/commit/0837fb97db0099246b6a0ad396d8d16af508ce6a))
* preserve bulk operation results ([#16](https://github.com/dstockton/qdrant-es-gateway/issues/16)) ([e0ed4fc](https://github.com/dstockton/qdrant-es-gateway/commit/e0ed4fc7bb11981a03c4410eb294cc02c4fda8c2))
* preserve mappings during updates ([24227bb](https://github.com/dstockton/qdrant-es-gateway/commit/24227bb2da7bfab07b05638b17acb6bb0dd5b3b7))
* preserve mappings during updates ([2bb49f2](https://github.com/dstockton/qdrant-es-gateway/commit/2bb49f2f5bf7ee3c24fe5917a3f2a29bdac7e74a))
* preserve source during partial updates ([#15](https://github.com/dstockton/qdrant-es-gateway/issues/15)) ([1af5aaf](https://github.com/dstockton/qdrant-es-gateway/commit/1af5aaf7c62f4d9d08276e2eac2b0f8877383e37))
* recursively merge partial updates ([#18](https://github.com/dstockton/qdrant-es-gateway/issues/18)) ([07d1c6e](https://github.com/dstockton/qdrant-es-gateway/commit/07d1c6e758df7ef65ff198171039e328f32bca29))
* reject duplicate bulk creates ([#11](https://github.com/dstockton/qdrant-es-gateway/issues/11)) ([16143a8](https://github.com/dstockton/qdrant-es-gateway/commit/16143a893dbd0295121612819559a4e047b78483))
* reject invalid UTF-8 request bodies ([03f76eb](https://github.com/dstockton/qdrant-es-gateway/commit/03f76eb3e6a0febf8840590d5b699e72087dad88))
* report accurate bulk index results ([6516a9b](https://github.com/dstockton/qdrant-es-gateway/commit/6516a9b9ff93924004fccd7fbdff7a930c6ec55e))
* report accurate standalone index results ([26357e5](https://github.com/dstockton/qdrant-es-gateway/commit/26357e5291c5369593fd41bf53852b4dfb3e2677))
* report accurate standalone index results ([e0bcd8d](https://github.com/dstockton/qdrant-es-gateway/commit/e0bcd8d24f027ec15a4670cee50d18a9e35bc0c5))
* require ids for bulk mutations ([4925928](https://github.com/dstockton/qdrant-es-gateway/commit/4925928c84f15fd610d18ba0ccc0444f65bd88d9))
* skip unchanged update writes ([b9e07fb](https://github.com/dstockton/qdrant-es-gateway/commit/b9e07fb737a351548c5f8be7d0504822ec40a586))
* support zero-size searches ([b93503a](https://github.com/dstockton/qdrant-es-gateway/commit/b93503ac685653632ed9c3250ae24ac32aff28bc))
* validate bulk action metadata ([df303e4](https://github.com/dstockton/qdrant-es-gateway/commit/df303e4a8ae58ceabe6e53cb7129846dac0d507e))
* validate bulk action metadata ([d3d51e5](https://github.com/dstockton/qdrant-es-gateway/commit/d3d51e5497ae3c76416d9fa9d381a4d01ea22569))

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

- Recursively merge inner objects during partial document updates so changing one nested member preserves its siblings, while arrays and scalar values retain Elasticsearch's replacement behavior.
- Support a separate `upsert` source in standalone and bulk partial updates, and avoid a redundant second existence read when a missing document is created by either supported upsert form.
- Preserve successful bulk `create`, `update`, and `delete` operation metadata, including the concrete index name when requests use an alias.
- Preserve the complete `_source` and refresh mapped filter/sort payload fields when an embedded-storage partial update changes a non-text field.
- Report HTTP 201 with `result: "created"` and HTTP 200 with `result: "updated"` for successful bulk index items while preserving one batched Qdrant lookup and write per contiguous index batch.
- Report HTTP 201 with `result: "created"` for new standalone index requests and HTTP 200 with `result: "updated"` when the ID already exists, including requests routed through aliases.
- Support `PUT` and `POST /<index>/_create/<id>` with HTTP 201 for a new document and an Elasticsearch-shaped HTTP 409 response without a write when the ID already exists; structured errors now retain the product header.
- Return an item-level `version_conflict_engine_exception` without overwriting the stored document when a bulk `create` action targets an existing ID.
- Route document and search requests made through a durable alias to the concrete index's Qdrant collections and deterministic ID namespace; alias inspection now returns the concrete-index response shape.
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
