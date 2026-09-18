---
layout: post
title: "Launch video: keep the Elasticsearch client, change the engine"
date: 2026-09-18
categories: [launch]
---

Here is the short launch video for Qdrant ES Gateway:

<video controls preload="metadata" poster="{{ '/assets/qdrant-es-gateway-launch.jpg' | relative_url }}" width="960">
  <source src="{{ '/assets/qdrant-es-gateway-launch.mp4' | relative_url }}" type="video/mp4">
  Your browser does not support embedded video. [Download the MP4]({{ '/assets/qdrant-es-gateway-launch.mp4' | relative_url }}).
</video>

The story is simple: an existing application keeps its Elasticsearch-shaped client requests, while the gateway translates the supported application-search subset to the Qdrant engine. The video shows the practical trade-offs—compatibility, retrieval, mixed workloads, memory, and the deliberately explicit feature boundary.

## How the video was made

AI was used as a production assistant, not as a substitute for the evidence. It helped turn benchmark results and compatibility notes into a tighter narrative, propose scene beats, catch confusing request-shape changes, and iterate on timing, layout, and captions. A local voice profile provided the narration, with repeated passes to fix pronunciation, pacing, volume consistency, and the final pause.

The visuals were then checked against the actual gateway behavior and benchmark outputs. That distinction matters: the story is polished, but the numbers and compatibility claims still come from the project’s tests and documentation.

See the [compatibility matrix]({{ '/compatibility/' | relative_url }}) and [production notes]({{ '/production/' | relative_url }}) for the detail behind the demo.
