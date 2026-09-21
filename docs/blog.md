---
layout: page
title: Blog
---

Short notes on compatibility, benchmarks, and the odd things that turn up in real search data.

{% for post in site.posts %}
## [{{ post.title }}]({{ post.url | relative_url }})

{{ post.excerpt | strip_html }}

{% endfor %}
