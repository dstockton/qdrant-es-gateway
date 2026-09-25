---
layout: post
title: "Dependabot can finish the job"
date: 2026-09-25
categories: [production, maintenance]
---

Dependency updates used to stop at “all checks passed”, waiting for someone to click merge.

That last click is now automated for Dependabot. The gateway waits for the full CI workflow—Rust tests and Clippy, dependency audit, Helm validation, container build, and vulnerability scan—to pass. Only then does it enable squash auto-merge, and only for a PR authored by Dependabot.

Human review remains the default for everything else. Routine maintenance gets to be routine.
