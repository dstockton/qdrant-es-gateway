---
layout: post
title: "A locked front door for routine maintenance"
date: 2026-09-25
categories: [production, maintenance]
---

Automatic dependency updates are useful right up until they can skip the tests.

`main` is now protected. A change needs a pull request, and Rust tests, Clippy, the dependency audit, Helm validation, and the container security scan must all pass before it can land.

That gives Dependabot a safe fast lane: it can merge boring, green updates by itself, while code changes and anything that fails a check still wait for a human. The guardrail is simple, visible, and enforced by GitHub—not by a promise in a workflow file.
