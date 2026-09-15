# Security

The gateway does not implement Elasticsearch Security or authorization. Put it behind an authenticated network boundary. Qdrant API keys are supplied through `QDRANT_API_KEY`, sent upstream only, and not logged. Set request and bulk limits appropriate to the deployment.
