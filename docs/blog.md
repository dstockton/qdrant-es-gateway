---
layout: page
title: Blog
---

{% for post in site.posts %}
## [{{ post.title }}]({{ post.url | relative_url }})

{{ post.excerpt | strip_html }}

{% endfor %}

