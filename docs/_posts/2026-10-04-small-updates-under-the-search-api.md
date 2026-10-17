---
layout: post
title: "Small updates under the search API"
date: 2026-10-04
categories: [production, maintenance]
---

Yesterday's [dependency refresh](https://github.com/dstockton/qdrant-es-gateway/pull/28) brought Qdrant from 1.15.3 to 1.15.5. The patches fix full-text index loading, snapshot corruption races, and scroll deadlocks. Staying on the 1.15 line keeps this change close to the gateway's tested behavior; Elasticsearch remains at the 8.15.0 comparison baseline.

The same change updated the SBOM action to 0.24.3, with newer Syft cataloguing and parser fixes. [CodeQL's scan-upload action](https://github.com/dstockton/qdrant-es-gateway/pull/24) also moved to 4.38.2. Both changes passed CI, including Rust tests, dependency audit, Helm validation, and the container vulnerability gate. The Qdrant refresh also passed a live gateway smoke test and produced valid source and image inventories.

That verification has limits. At the time of the refresh, the unchanged application replay passed 5 of 12 checks on both Qdrant versions; allowing write visibility and refreshing Elasticsearch after deletion raised both to 9 of 12. This established parity for that comparison, not complete Elasticsearch compatibility. Fresh-instance tests also do not prove rolling-upgrade or rollback safety: keep a backup before upgrading existing Qdrant storage.
