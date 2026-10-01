---
layout: post
title: "Alias batches are one routing change"
date: 2026-10-01
categories: [compatibility, correctness]
---

Elasticsearch's aliases API accepts a list of actions so clients can express a routing change as one request. The gateway previously applied each action to its SQLite catalogue while it was still parsing later actions. A valid `add` followed by a malformed `remove` could therefore return HTTP 400 after the new alias was already visible.

The gateway now validates every action first. Each item must contain exactly one supported `add` or `remove` action with object metadata and string `index` and `alias` values. Only after all target indices and alias-name conflicts have passed validation does one SQLite transaction apply the complete list. Removal also matches both alias and concrete index, rather than deleting an alias that points somewhere else.

The regression first submits a valid add followed by a remove with no index and verifies that no alias was added. It then switches an existing alias from `products-v1` to `products-v2` with a two-action request and verifies the committed target:

```sh
cargo test alias_actions_validate_before_an_atomic_catalogue_update
```

This guarantee covers the gateway's local metadata catalogue. The current alias model still has one target per alias; it does not implement filtered aliases, multi-index fan-out, write-index selection, wildcard actions, or a distributed transaction with Qdrant. Deployments with independent SQLite catalogues must still coordinate alias changes outside the gateway.
