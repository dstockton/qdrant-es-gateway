# Storage findings

The storage comparison must include all durable data required by each architecture. For the gateway projection profile that means both Qdrant collections: the searchable collection and the low-overhead source collection. Gateway SQLite metadata is reported separately. Docker image layers and unused volume capacity are excluded.

## Fresh 500,000-document run

The containerized benchmark used the repository's standard profile, 500,000 generated products, 5,000-document bulk batches, 5,000 updates, and the two-collection gateway projection. The final mounted-directory measurements were:

| System | Storage | Size |
|---|---|---:|
| Native Elasticsearch | data directory | 16,500 KiB / 16.1 MiB |
| Gateway | Qdrant searchable collection | 502,064 KiB / 490.3 MiB |
| Gateway | Qdrant durable source collection | 209,088 KiB / 204.2 MiB |
| Gateway | gateway metadata | 24 KiB |
| Gateway total | searchable + source + metadata | 711,176 KiB / 694.5 MiB |

The complete gateway footprint was therefore approximately 43.1× the native Elasticsearch data directory in this run. This is not a defect to hide: Qdrant's segment and sparse-vector representation has a much larger fixed and per-document footprint for this small product schema, while Elasticsearch's on-disk representation is highly compact. The gateway's source projection is still useful for write-path isolation and stateless reads, but it is not currently a disk-saving strategy.

The same run measured 46.689 seconds versus 105.141 seconds for bulk setup, 191.52 ms versus 986.63 ms search P95 at 50 clients, and 522.68 versus 95.60 requests/second for the mixed workload. Single-document updates remained slower on the gateway. Both paths returned full source responses, but the top-ranked IDs were not identical.

## Design implication

The current recommendation is workload-dependent:

- Choose the gateway projection when search latency, mixed read throughput, memory headroom, and stateless horizontal scaling matter more than raw disk footprint.
- Keep native Elasticsearch as the stronger baseline when storage cost is dominant or ranked-result equivalence is a hard requirement.
- Do not enable a durable source projection merely to reduce storage. Enable it to isolate sparse-index work from source reads and to support asynchronous/eventual update strategies.
- For production sizing, repeat after Qdrant compaction and with the actual schema, payload indexes, replication factor, and retention policy. Update-heavy workloads must include post-compaction measurements because segment churn can temporarily inflate the footprint.

The reproducible collector is [benchmark/disk_usage.sh](../benchmark/disk_usage.sh). It measures mounted directories while the stack is running, so the result is tied to the actual persisted corpus rather than container limits or image size.
