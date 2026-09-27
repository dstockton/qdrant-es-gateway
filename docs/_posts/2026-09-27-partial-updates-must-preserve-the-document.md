---
layout: post
title: "A partial update must preserve the document"
date: 2026-09-27
categories: [compatibility, correctness]
---

In the default embedded-source storage mode, a partial update to a non-text field took an optimized Qdrant payload path. That path wrote only the patch beneath `_source`. An update such as `{"doc":{"price":125}}` could therefore make the next document read return only `price`, dropping untouched fields such as `title` and `brand`. The mapped top-level `price` payload used by filters and sorts also remained stale.

The gateway already reads the current document to implement update semantics. It now merges the patch into that source and sends one payload update containing the complete `_source` plus the refreshed mapped filter/sort fields. The fix adds no Qdrant round trip and does not rebuild sparse vectors when no text field changed.

The regression starts with a four-field source and a mapped numeric price, applies a price-only patch, and inspects the Qdrant write. It verifies that the new price is present both in `_source` and the filter payload, while every untouched source field survives:

```sh
cargo test embedded_partial_update_preserves_source_and_refreshes_filter_payload
```

The trade-off is payload size: even a small non-text patch now transfers the merged source back to Qdrant. The update is also still a read followed by a write, not an atomic compare-and-write; concurrent updates can race, and the gateway does not yet implement Elasticsearch sequence-number concurrency controls.
