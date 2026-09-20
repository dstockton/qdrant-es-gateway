---
layout: post
title: "When bulk errors did not mean errors"
date: 2026-09-20
categories: [compatibility, correctness]
---

Elasticsearch bulk responses have two levels: a top-level `errors` boolean and an `items` array whose action objects contain per-document results. The [Bulk API contract](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-bulk) defines `errors` as true when one or more operations did not complete successfully. Clients commonly inspect the boolean first and only walk the item details when it is true.

The gateway already returned useful per-item failures, but calculated `errors` at the wrong JSON level. It looked for `error` directly on each item even though the field lives under the action name, such as `items[0].update.error`. As a result, a failed update or delete could be returned with `"errors": false`, encouraging a client to treat the batch as successful.

The response builder now checks every action result for a nested `error`. A focused regression test proves that a successful item leaves the flag false and a failed item sets it true.

To reproduce the verification:

```bash
cargo test bulk_response_reports_nested_item_errors
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

This change only corrects the summary flag. It does not add rollback or atomicity: bulk requests remain a sequence of operations, callers must still inspect failed items, and retry policy remains the caller's responsibility.
