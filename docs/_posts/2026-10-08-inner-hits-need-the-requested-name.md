---
layout: post
title: "Inner hits need the requested name"
date: 2026-10-08
---

The AWS Retail Demo Store sends a normal Elasticsearch collapse request: collapse on `category.keyword`, name the inner result `category_hits`, request a bounded number of group members, and suppress `_source`. Its response code then reads `item["inner_hits"]["category_hits"]` to distribute products across categories. The request is visible in the project's [search service](https://github.com/aws-samples/retail-demo-store/blob/master/src/search/src/search-service/app.py#L130-L158), and the corresponding response access is a few lines below.

The gateway previously treated every property inside the `inner_hits` definition as if it were an inner-hit name. A request containing `name`, `size`, `fields`, and `_source` therefore returned groups with those literal keys instead of `category_hits`. It also left `_source` in nested hits even when the inner definition disabled it. The request returned HTTP 200, but the unchanged application could not consume the response.

Collapse parsing now happens before index lookup or Qdrant access. The gateway emits the requested inner-hit name, honors the inner size and source projection independently from the top-level projection, accepts the application's `_id` metadata-field request, and caps inner results with `MAX_PAGE_SIZE`. Unsupported options fail with a structured 400 rather than being silently ignored.

The regression uses the public application's exact collapse shape over three mocked products in two categories. It verifies two collapsed top-level hits, a two-document `category_hits` group, correct IDs, no leaked `_source`, and zero Qdrant requests for invalid or over-limit collapse options:

```sh
cargo test retail_collapse_inner_hits_use_the_requested_name_size_and_source_filter
cargo test invalid_collapse_fails_before_qdrant_work
```

This remains deliberately bounded compatibility. The gateway currently accepts one inner-hit definition, does not implement inner-hit sorting, and groups only the retrieved candidate window. Group totals are exact within that window, not across all matches in a large index. Those limits are now explicit instead of being hidden behind a response shape that the target application could not read.
