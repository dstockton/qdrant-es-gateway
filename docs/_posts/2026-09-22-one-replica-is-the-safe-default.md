---
layout: post
title: "One replica is the safe default"
date: 2026-09-22
---

The Helm chart used to request two gateway replicas while also provisioning one `ReadWriteOnce` volume for the SQLite metadata database. That combination looked highly available but did not supply a highly available control plane.

SQLite stores index mappings, vector definitions, and aliases. With a typical `ReadWriteOnce` volume, two pods may be constrained to one node or the second pod may be unable to mount the volume. Giving each pod separate storage is worse: the replicas can disagree about which indices and aliases exist. Even a shared filesystem needs locking semantics suitable for SQLite, and concurrent index administration still crosses SQLite and Qdrant without a distributed transaction.

The chart now defaults to one replica. CI renders the Deployment and asserts that default, while the chart and production guides spell out the requirements before an operator overrides it. This does not reduce a working default deployment from two fault-independent replicas: the old defaults never provided that guarantee.

To reproduce the chart check:

```sh
helm lint deploy/helm/qdrant-es-gateway
helm template smoke deploy/helm/qdrant-es-gateway \
  --show-only templates/deployment.yaml | grep '^  replicas:'
```

The expected rendered value is `replicas: 1`.

This is a safer default, not horizontal-availability support. A production design that needs multiple gateway replicas should first move the small metadata catalog to a replicated control-plane store, or deliberately provide `ReadWriteMany` storage with verified SQLite locking and serialize index and alias changes externally. Qdrant replication protects document collections; it does not replicate the gateway's SQLite catalog.
