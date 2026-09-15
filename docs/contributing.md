# Contributing

Install Rust, Docker, and run `make test` and `make lint`. Integration tests require Docker and a reachable Qdrant. Keep unsupported behavior explicit: do not silently discard an ES clause. Add a unit test for each new translator rule and an integration fixture for each Qdrant API dependency.
