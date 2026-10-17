---
layout: page
title: Blog
---

Short notes on compatibility, benchmarks, and the odd things that turn up in real search data.

Prefer plain text? Browse the [Markdown index]({{ '/llms.txt' | relative_url }}) or [complete text export]({{ '/llms-full.txt' | relative_url }}).

{% for post in site.posts %}
## [{{ post.title }}]({{ post.url | relative_url }})

[Markdown]({{ '/markdown/' | append: post.path | remove: '_posts/' | relative_url }})

{{ post.excerpt | strip_html }}

{% endfor %}
