---
layout: post
title: "TLS fixes alongside the shared catalog"
date: 2026-10-15
categories: [maintenance, security]
---

The shared-catalog change in [PR #64](https://github.com/dstockton/qdrant-es-gateway/pull/64) also moved Rustls from 0.23.45 to 0.23.46. That patch is now on main. It strengthens the TLS layer used for HTTPS requests and encrypted catalog connections, without changing the gateway's search API.

The [upstream release](https://github.com/rustls/rustls/releases/tag/v/0.23.46) fixes several security issues, including secret data appearing in session debug output and key-exchange hints being saved before a handshake is confirmed. Some other fixes concern optional TLS features; this does not mean every deployment was exposed to every issue.

The merged change passed Rust tests, the dependency audit, Helm checks, and the container build and vulnerability gate. Today's follow-up also passed formatting, tests, Clippy, a refreshed dependency audit, and Compose and Helm validation. These checks support the update; they are not a reproduction of each upstream security advisory.
