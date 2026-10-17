---
layout: post
title: "Keeping image scan results visible"
date: 2026-10-16
categories: [maintenance, security]
---

[PR #69](https://github.com/dstockton/qdrant-es-gateway/pull/69) moved the CodeQL Action from 4.38.2 to 4.38.3 in CI and release builds. The update landed yesterday. We use its SARIF uploader to send Trivy's container scan results to GitHub, keeping those findings available alongside the build.

The [upstream patch](https://github.com/github/codeql-action/releases/tag/v4.38.3) updates the default CodeQL bundle and fixes bundle selection for custom query configurations. Those analysis features are separate from our uploader use; this is routine maintenance, with no change to the gateway's search behavior or vulnerability threshold.

The merged change passed documentation checks, Rust tests, the dependency audit, Helm validation, and the container build and enforced vulnerability gate. The separate Trivy reporting check was neutral. Today's follow-up also passed formatting, Rust tests, Clippy, a refreshed dependency audit, and Compose and Helm checks.
