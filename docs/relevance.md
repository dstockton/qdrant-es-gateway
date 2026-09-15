# Relevance evaluation

Use a labelled query/corpus fixture and compare top-k IDs with Elasticsearch. Track MRR or NDCG, not raw score equality. Differences can come from analyzers, tokenization, field fusion, document length normalization, and BM25 implementation. A production migration should include domain-labelled queries before making an economics decision.
