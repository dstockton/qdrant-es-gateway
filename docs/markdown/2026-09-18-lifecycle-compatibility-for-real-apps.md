# Compatibility includes the boring lifecycle calls

An application can have a perfectly ordinary search query and still fail a migration before it reaches that query. Production clients create and delete indexes, ask whether an index exists, refresh after an import, and sometimes close and reopen an index while applying settings.

Qdrant ES Gateway accepts `_refresh`, `_open`, `_close`, and basic `_settings` lifecycle calls for client and application compatibility. Accepting these request shapes does not reproduce Elasticsearch's refresh, open/close, or settings semantics. Qdrant collections remain continuously available underneath.

Settings and analysis are different. The gateway durably records mapping metadata, but it does not emulate Elasticsearch analyzer or settings semantics. Analyzer settings that change query semantics remain unsupported. Applications that depend on those behaviors need to check compatibility before migrating.

These calls can reduce changes to a catalogue bootstrap when its lifecycle requests fit the supported subset. The team still needs to check whether the application relies on the effects of those calls and test search behavior separately. The [compatibility matrix](https://dstockton.github.io/qdrant-es-gateway/compatibility/) lists the supported surface and its limits.
