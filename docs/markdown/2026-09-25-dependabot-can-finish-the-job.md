# Dependabot can finish the job

Dependency updates used to stop at “all checks passed”, waiting for someone to click merge.

That last click is now automated for Dependabot. The gateway waits for the full CI workflow—Rust tests and Clippy, dependency audit, Helm validation, container build, and vulnerability scan—to pass. Only then does it enable GitHub’s squash auto-merge for a PR authored by Dependabot.

Human review remains the default for everything else. Routine maintenance gets to be routine.
