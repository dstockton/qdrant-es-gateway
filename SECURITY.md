# Security

The gateway does not implement Elasticsearch Security or authorization. Put it behind an authenticated network boundary. Qdrant API keys are supplied through `QDRANT_API_KEY`, sent upstream only, and not logged. Set request and bulk limits appropriate to the deployment.

## Supported versions

Only the latest release on the default branch is actively maintained. Container images are immutable by version tag and should be promoted by digest.

## Reporting a vulnerability

Please do not open a public issue for a suspected vulnerability. Use GitHub's private security advisory workflow for this repository, or contact the maintainer privately through the address on the GitHub profile. Include the affected version, a minimal reproduction, impact, and any suggested mitigation. Do not include credentials, private Qdrant URLs, customer data, voice references, or generated media in a report.

Security fixes are published with a changelog entry and a new container tag when practical.
