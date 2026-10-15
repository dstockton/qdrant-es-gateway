---
layout: post
title: "Reading Reddit with an honest coverage report"
date: 2026-10-15
categories: [engineering]
---

A daily search investigation is only useful if we know what it actually read. Anonymous Reddit feeds returned HTTP 403 during ours. Cached search snippets sometimes showed conflicting relative dates. Neither was enough to establish that a report had been published in the previous 24 hours.

The repository now includes a small, read-only OAuth collector. Once Reddit approves the use and an account supplies a token, it walks the new-submission listings for Elasticsearch and OpenSearch, follows pagination, and selects reports using their exact UTC creation timestamps. Duplicate IDs, removed posts, and old pinned entries do not become fresh reports.

Run `python3 scripts/reddit_reports.py --metadata-only` to get a coverage manifest for the preceding 24 hours. Each subreddit records how many pages were read and why reading stopped. A page cap, repeated cursor, or access error produces an incomplete status and a failing exit code. An exhausted listing means that accessible listing ended; it does not mean every Reddit conversation was reviewed.

The offline tests exercise pagination, time boundaries, duplicate reports, deleted bodies, and partial listings. Live access has not been verified. Reddit says, [“Clients must authenticate with a registered OAuth token.”](https://support.reddithelp.com/hc/en-us/articles/16160319875092-Reddit-Data-API-Wiki) Its [Responsible Builder Policy](https://support.reddithelp.com/hc/en-us/articles/42728983564564-Responsible-Builder-Policy) also requires approval for the use case. The collector cannot grant that approval.

There is no persistent content cache. Titles and bodies are available for immediate review, while metadata-only output omits them. Anyone retaining output or publishing a quotation must handle deletion and the applicable retention terms. The [setup guide](https://github.com/dstockton/qdrant-es-gateway/blob/main/research/README.md) includes a transparent access-request description covering automated technical assessment and possible articles.

This improves the evidence trail, not the gateway's query speed. When a source remains inaccessible, the investigation will say so instead of treating an empty search result as proof that nobody had a problem.
