# Internals

The current implementation uses direct Qdrant REST calls to keep the protocol boundary visible and easy to audit. The metadata store is SQLite because it is durable, embedded, and small enough for index/mapping/alias metadata. It is not a document store.
