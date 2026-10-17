# Index names are a client contract

An invalid index name used to make it through the gateway's metadata catalogue and into a normalized Qdrant collection name. That was more than cosmetic. An application could successfully provision state that the same Elasticsearch or OpenSearch client would expect a native cluster to reject, and distinct punctuation could then collide in Qdrant's namespace.

Elasticsearch and OpenSearch implement their validators independently, so this change checked both rather than assuming the fork still behaved like its ancestor. [Elasticsearch 9.5.5's validator](https://github.com/elastic/elasticsearch/blob/v9.5.5/server/src/main/java/org/elasticsearch/cluster/metadata/MetadataCreateIndexService.java#L342-L365) and [OpenSearch 3.9's validator](https://github.com/opensearch-project/OpenSearch/blob/3.9/server/src/main/java/org/opensearch/cluster/metadata/MetadataCreateIndexService.java#L313-L342) agree on the static boundary:

- a name must be non-empty and lowercase;
- filesystem-reserved punctuation, `#`, and `:` are forbidden;
- `_`, `-`, and `+` cannot be the first character;
- the UTF-8 encoding cannot exceed 255 bytes; and
- `.` and `..` are invalid.

The byte boundary matters for non-ASCII names: 127 copies of `é` occupy 254 UTF-8 bytes and pass, while 128 copies occupy 256 bytes and fail. The source implementations permit `+` away from the first character, despite the current OpenSearch prose documentation listing `+` among generally forbidden characters. The gateway follows the versioned 3.9 implementation for that edge rather than broadening the rejection.

Invalid create requests now return HTTP 400 with the client-recognizable `invalid_index_name_exception`, root cause, index name, and `_na_` index UUID. Validation runs under the existing index-administration lock but before SQLite reads and Qdrant requests. Focused regressions cover the UTF-8 byte limit, punctuation, prefixes, lowercase requirement, response shape, and a zero-request mock upstream counter:

```console
cargo test index_name_validation_matches_shared_native_rules
cargo test invalid_index_creation_fails_before_qdrant_work
```

This run is source- and request-level validation, not a live-engine benchmark. The local Docker service did not answer its availability check, so no native containers were claimed. Dot-prefixed names other than the exact `.` and `..` remain accepted by both versioned static validators; their separate hidden/system-index checks and deprecation behavior are not emulated by the gateway.
