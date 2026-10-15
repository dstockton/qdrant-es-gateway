---
layout: post
title: "Documentation you can read as text"
date: 2026-10-15
categories: [documentation]
---

The blog works well in a browser. A coding assistant reviewing a migration needs something simpler: the compatibility boundary, the deployment instructions, and article text without navigation markup.

Every article now has a downloadable Markdown copy. The [llms.txt index]({{ '/llms.txt' | relative_url }}) starts with the compatibility and production guides, then links to all articles. [llms-full.txt]({{ '/llms-full.txt' | relative_url }}) puts those guides and the articles into one text file, with source links separating them.

These files are generated from the same Markdown sources that build the site. There is no second set of articles to maintain. Run `python3 scripts/export_agent_docs.py` after editing the documentation; `--check` detects stale copies in CI. The exports remove Jekyll front matter and resolve site links so a reader can follow them outside the rendered page.

The format follows the [llms.txt proposal](https://llmstxt.org/). It gives tools a predictable entry point; it does not guarantee that any crawler or assistant will discover, index, or follow it. Search engines can still use the normal HTML pages and sitemap.

For a migration review, start with the compatibility guide before reading a successful example. A post proves a particular workload, not full Elasticsearch or OpenSearch equivalence. Keeping that boundary beside the examples makes the repository easier to use responsibly, for people and agents alike.
