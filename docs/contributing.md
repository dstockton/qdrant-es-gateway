# Contributing

Install Rust, Docker, and run `make test` and `make lint`. Integration tests require Docker and a reachable Qdrant. Keep unsupported behavior explicit: do not silently discard an ES clause. Add a unit test for each new translator rule and an integration fixture for each Qdrant API dependency.

After changing an article or the compatibility, architecture, production, or migration guide, run `python3 scripts/export_agent_docs.py`. Commit the generated `docs/markdown/`, `docs/llms.txt`, and `docs/llms-full.txt` alongside the source changes. CI checks that they remain synchronized. Markdown exports have no front matter, so Jekyll copies them as literal text files.
