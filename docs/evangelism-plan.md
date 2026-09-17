# Open-source launch and evangelism plan

The goal is to earn technically credible attention, not to claim that the gateway is a drop-in replacement for every Elasticsearch workload. Lead with the narrow promise: keep the Elasticsearch client and REST request shape while using Qdrant-backed application search, with measured read/mixed-workload benefits and explicit compatibility boundaries.

## Launch sequence

### 1. Make the repository easy to evaluate

- Publish a tagged GHCR image and Helm chart.
- Keep the five-minute Docker Compose example working.
- Add a short compatibility matrix, architecture diagram, benchmark methodology, and reproducible commands.
- Publish benchmark result JSON and storage measurements, not just headline multipliers.
- Add an issue template for unsupported Elasticsearch requests and a roadmap label set.

### 2. Hacker News

Submit after a stable release and a clean README. Suggested title:

> Show HN: qdrant-es-gateway — keep the Elasticsearch API, use Qdrant for application search

Opening text:

> I built a small Rust gateway for catalogue, documentation, jobs, and ticket search. Existing Elasticsearch clients continue to send the same REST requests; the gateway translates the supported application-search subset to Qdrant. The repository includes a reproducible container benchmark, Helm chart, compatibility matrix, and the limitations I found. I’m especially interested in feedback on query semantics and where an application should stop using this approach and stay on Elasticsearch.

Do not lead with “replacement” or a single benchmark number. Reply with methodology, corpus size, request mix, storage measurements, and links to the exact code.

### 3. Reddit

Use communities selectively and follow each community's self-promotion rules. Start with r/rust for the implementation story, r/elasticsearch for compatibility feedback, and r/qdrant for backend/query-model feedback. Use distinct posts rather than cross-posting identical promotional copy.

Rust angle: “A Rust Elasticsearch-compatible gateway translating application search to Qdrant.”

Search angle: “What Elasticsearch application-search features do you consider essential before evaluating a compatibility gateway?”

Each post should ask one concrete question and include the limitations section.

### 4. YouTube

Produce three videos:

1. A 60–90 second launch video: request shape, architecture, and the measured headline.
2. A 6–8 minute technical walkthrough: Docker Compose, unchanged client, mappings, filters, `_source`, updates, and the two-collection option.
3. A benchmark deep dive: corpus size, workload mix, p50/p95, throughput, storage, and why the numbers are not universal capacity claims.

Use “Qdrant” on screen and “Quadrant” in narration. Put the benchmark commit, dataset generator, and commands in the description.

### 5. TikTok / short-form clips

Cut the long video into focused clips:

- “Same Elasticsearch request, different backend.”
- “Why two Qdrant collections can improve mixed workloads.”
- “The storage trade-off in one chart.”
- “What the gateway deliberately does not support.”

Keep each clip to one idea, show the request and response, and point to the full benchmark rather than making a standalone performance claim.

## Content calendar

| Day | Asset | Call to action |
|---|---|---|
| 0 | Release, README, launch video | Try the five-minute example |
| 1 | Hacker News submission | Review compatibility and methodology |
| 2 | Rust Reddit post | Discuss gateway implementation |
| 4 | Technical YouTube walkthrough | Reproduce the demo |
| 7 | Elasticsearch/Qdrant Reddit posts | Report missing request features |
| 10 | Benchmark deep-dive video | Compare with your corpus |
| 14 | Short-form clips | Share a concrete use case |

## Measurement and community loop

Track GitHub stars, forks, issues labelled `compatibility`, image pulls, Helm installs where observable, video retention, and the number of reproducible external benchmark reports. Treat feature requests as compatibility evidence: add a request fixture and semantic test before adding an implementation claim.

## Publishing boundary

The repository can prepare release assets, drafts, benchmark evidence, and video files. Final posts to Hacker News, Reddit, YouTube, or TikTok should be reviewed and published through the owner's accounts because they create external identity, moderation, and audience commitments.
