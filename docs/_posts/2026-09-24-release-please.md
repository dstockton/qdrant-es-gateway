---
layout: post
title: "Releases should not depend on remembering three version files"
date: 2026-09-24
categories: [production, releases]
---

The gateway has one release version, but it appears in more than one place: Rust, the Helm chart, and the lockfile.

That is exactly the sort of job a human does once, then forgets on a Friday afternoon.

Release Please now watches `main` and opens a release pull request when Conventional Commit messages add a feature or fix. The pull request updates the changelog and keeps the Rust and Helm versions together. Once it is reviewed and merged, Release Please creates the tag and GitHub release; the existing publish pipeline sees that tag and adds the container, chart package, SBOM, and attestation.

The useful part is the pause in the middle: automation prepares the release, but a person still gets to read it before anything ships.
