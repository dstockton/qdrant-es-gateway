# Read/write crossover

For product search, updates do not all need to block the user-facing request. A price or stock update can be accepted, applied asynchronously, and become searchable shortly afterwards. That makes the useful question less binary than “can the gateway beat a native write?”: how much of the workload is customer-facing retrieval, and how much is synchronous mutation?

## Focused mix sweep

The sweep used the same Elasticsearch-shaped client calls against both services:

- 50,000 product documents
- 1,000 requests per point
- 50 concurrent clients
- read/search traffic split evenly between `_search` and `GET _doc`
- write traffic made up of single-document `_update` calls
- `ASYNC_PAYLOAD_WRITES=true` for the gateway

| Read/search share | Write/update share | Elasticsearch throughput | Qdrant gateway throughput | Approximate winner |
|---:|---:|---:|---:|---|
| 20% | 80% | 1,166.90 req/s | 962.31 req/s | Elasticsearch |
| 40% | 60% | 1,080.77 req/s | 1,177.88 req/s | Qdrant gateway |
| 60% | 40% | 1,071.20 req/s | 1,059.19 req/s | Near tie |
| 80% | 20% | 1,091.95 req/s | 1,308.47 req/s | Qdrant gateway |

The practical crossover is therefore around 40–60% read/search traffic for this profile. At roughly 40% reads, the gateway begins to lead on throughput; at 60%, the two paths are effectively tied; and at 80%, the gateway has a clear throughput advantage. This is a workload boundary, not a universal constant: corpus size, query shape, update fields, concurrency, and the freshness window all move it.

The 500,000-document mixed run reinforces the shape of the result. With 70% search/read and 30% update/delete traffic, throughput was 77.42 req/s for Elasticsearch and 263.32 req/s for the gateway. Search p95 was 4,213.6 ms versus 305.82 ms. The gateway’s asynchronous update mode is a good fit when the product can tolerate eventual search visibility for ordinary catalog changes.

![Bubble chart comparing corpus size, search p95 latency, and throughput](assets/throughput-latency-corpus-bubbles.png)

## How to use the crossover

For a catalog workload dominated by imports, synchronous writes, or strict read-after-write requirements, native Elasticsearch remains the safer baseline. For a product experience dominated by retrieval—especially when updates can flow through an event queue or tolerate a short freshness delay—the gateway becomes increasingly attractive.

The next production decision should be based on the application’s freshness contract: measure the acceptable update-to-search delay, then place the workload on this curve using the same query mix and concurrency as the real service.

## Additional corpus scaling points

To make the bubble chart less dependent on the two original points, the same mixed workload was repeated at four smaller corpus sizes. These runs used the current gateway build, 50 concurrent clients, 1,000 mixed requests, and the existing 70% read/search versus 30% update/delete composition.

| Corpus | Elasticsearch throughput | Qdrant gateway throughput | Elasticsearch mixed search p95 | Qdrant gateway mixed search p95 |
|---:|---:|---:|---:|---:|
| 10,000 | 749.48 req/s | 1,192.41 req/s | 125.06 ms | 97.10 ms |
| 25,000 | 982.57 req/s | 1,011.40 req/s | 101.34 ms | 97.23 ms |
| 50,000 | 1,037.89 req/s | 1,010.64 req/s | 96.90 ms | 101.20 ms |
| 100,000 | 935.91 req/s | 986.74 req/s | 103.94 ms | 102.97 ms |
| 500,000 | 77.42 req/s | 263.32 req/s | 4,213.60 ms | 305.82 ms |

The 10k–100k points were collected in the local four-CPU container profile. The 500k point is the previously captured production-shaped run, so it is useful for scale context but should not be treated as a perfectly controlled continuation of the smaller points. That distinction is called out directly in the chart.
