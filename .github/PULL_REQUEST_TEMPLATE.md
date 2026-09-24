## What changed?

<!-- Explain the change and the user or operator problem it addresses. -->

## How was it tested?

<!-- List commands, benchmarks, or manual checks. Include compatibility evidence when relevant. -->

## Compatibility and operational impact

- [ ] Existing Elasticsearch-shaped client request shapes are unchanged, or the compatibility impact is documented.
- [ ] New configuration, storage, deployment, or resource requirements are documented.
- [ ] Error and unsupported-feature behavior is covered where relevant.

## Checklist

- [ ] Tests pass locally.
- [ ] `cargo fmt --check` passes.
- [ ] `cargo clippy --all-targets --all-features -- -D warnings` passes, when applicable.
- [ ] Documentation or compatibility matrix updated, when applicable.
- [ ] No credentials, private data, generated secrets, or private tooling are included.
- [ ] The change is scoped to this pull request.
