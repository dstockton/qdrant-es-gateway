---
layout: post
title: "Three nodes, one disappears, and the search keeps moving"
date: 2026-09-19
categories: [resilience, benchmarking]
---

A search service is easy to benchmark when everyone is alive. The more useful question is what happens when one of them vanishes in the middle of lunch.

I ran the same Elasticsearch-shaped client workload against two small clusters: three Elasticsearch nodes, and three Qdrant nodes behind three Qdrant Elasticsearch Gateway replicas. Both sides used replicated data, a load balancer, six concurrent workers, and a 1,000-document catalogue. Three seconds into each 12-second run, one storage node was hard-killed and then restarted.

| workload | Elasticsearch | Qdrant + gateway |
| --- | ---: | ---: |
| search throughput | 145 req/s | 1,261 req/s |
| insert throughput | 665 req/s | 1,169 req/s |
| update throughput | 1,952 req/s | 1,335 req/s |
| delete throughput | 979 req/s | 1,417 req/s |
| mixed throughput | 1,183 req/s | 1,296 req/s |
| recovery time | 4.4–4.6 s | 4.5–4.7 s |

Losing a node did not turn either service into a pumpkin. The gateway path kept serving the mixed workload at roughly the same rate as native Elasticsearch, and was faster for inserts and deletes in this run. Native Elasticsearch was faster for the update-heavy case. On both sides, the main cost was a handful of requests waiting for the dead node to time out; p95 latency stayed low outside that interruption.

In the mixed case, the gateway was about 9% faster. Inserts were about 76% faster and deletes about 45% faster; the update-heavy case went the other way by roughly 32%. The search-only result was especially stark in this small, CPU-constrained run, with the gateway at about 8.7× the observed throughput.

A useful engineering discovery came from the test. A load balancer must check whether a gateway process is alive, not whether its upstream storage is having a bad five seconds. Using the gateway’s strict readiness check made every replica disappear during a Qdrant node restart. Switching the load balancer to the liveness check kept the gateway tier in rotation while Qdrant recovered. That is the difference between “the database is repairing itself” and “the entire API is down.”

This harness shares the gateway’s SQLite metadata volume between replicas so the test can focus on the data path. The current control plane is not fully stateless; the next production step is to move that small index-metadata responsibility to a shared control-plane store.

The reproducible profile and raw JSON are in [`benchmark/resilience`](https://github.com/dstockton/qdrant-es-gateway/tree/main/benchmark/resilience). The client still sends the same requests; only the endpoint changes.
