#!/usr/bin/env python3
"""Identical deterministic requests against OpenSearch and gateway endpoints."""
import argparse
import json
import time
import urllib.error
import urllib.request


def request(endpoint, method, path, body=None):
    data = body.encode() if isinstance(body, str) else json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(endpoint + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as response:
        return json.load(response)


def run(endpoint):
    index = "explicit-sparse-demo"
    try:
        request(endpoint, "DELETE", "/" + index)
    except urllib.error.HTTPError as error:
        if error.code != 404:
            raise
    request(endpoint, "PUT", "/" + index, {
        "settings": {"index.sparse": True, "number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {"properties": {"v": {"type": "sparse_vector", "method": {"name": "seismic", "engine": "lucene"}}}}})
    tokens = [0, 1, 32767, 32768, 40342, 65535, 65536, 105878]
    bulk = "".join(json.dumps({"index": {"_id": str(token)}}) + "\n" + json.dumps({"v": {str(token): 1.0}}) + "\n" for token in tokens)
    result = request(endpoint, "POST", f"/{index}/_bulk", bulk)
    assert not result["errors"], result
    request(endpoint, "POST", f"/{index}/_refresh")
    results = []
    for token in tokens:
        start = time.perf_counter()
        result = request(endpoint, "POST", f"/{index}/_search", {
            "size": 10, "_source": False,
            "query": {"neural_sparse": {"v": {"query_tokens": {str(token): 1.0}}}}})
        elapsed_ms = (time.perf_counter() - start) * 1000
        ids = [hit["_id"] for hit in result["hits"]["hits"]]
        assert ids == [str(token)], (endpoint, token, ids)
        results.append({"token": token, "ids": ids, "elapsed_ms": round(elapsed_ms, 3)})
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--opensearch", default="http://localhost:19290")
    parser.add_argument("--gateway", default="http://localhost:19291")
    args = parser.parse_args()
    print(json.dumps({"opensearch": run(args.opensearch), "gateway": run(args.gateway)}, indent=2))
