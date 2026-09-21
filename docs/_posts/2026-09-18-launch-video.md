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

The story is simple: an existing application keeps its Elasticsearch-shaped client requests, while the gateway translates the supported application-search subset to the Qdrant engine. The video compares compatibility, retrieval, mixed workloads, memory, and the supported feature boundary.

## How the video was made

AI helped with the production work: shaping benchmark results into a narrative, suggesting scene beats, catching confusing request-shape changes, and iterating on timing, layout, and captions. A local voice profile provided the narration; several passes fixed pronunciation, pacing, volume, and the final pause.

The visuals were checked against the gateway and benchmark outputs. The numbers and compatibility claims come from the project’s tests and documentation.

See the [compatibility matrix]({{ '/compatibility/' | relative_url }}) and [production notes]({{ '/production/' | relative_url }}) for the detail behind the demo.
