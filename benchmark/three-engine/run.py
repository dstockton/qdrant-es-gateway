#!/usr/bin/env python3
"""Identical endpoint-only requests, deterministic expected results, raw trial data."""
import concurrent.futures
import datetime
import http.client
import json
import math
import os
import random
import statistics
import subprocess
import threading
import time
import urllib.parse
from pathlib import Path

N = int(os.getenv('DOCS', '10000'))
BATCH = int(os.getenv('BATCH', '500'))
REQUESTS = int(os.getenv('REQUESTS', '100'))
TRIALS = int(os.getenv('TRIALS', '5'))
CONCURRENCY = [int(x) for x in os.getenv('CONCURRENCY', '1,8').split(',')]
SETTLE = int(os.getenv('SETTLE_SECONDS', '15'))
OUTPUT = Path(os.getenv('OUTPUT', 'results/three-engine.json'))
INDEX = 'balanced_benchmark'
ENDPOINTS = {'elasticsearch': os.getenv('ES_URL', 'http://localhost:19200'),
             'opensearch': os.getenv('OS_URL', 'http://localhost:19201'),
             'gateway': os.getenv('GATEWAY_URL', 'http://localhost:19202')}
MAPPING = {'settings': {'number_of_shards': 1, 'number_of_replicas': 0},
           'mappings': {'properties': {'rank': {'type': 'integer'}, 'group': {'type': 'keyword'},
                                      'price': {'type': 'integer'}, 'description': {'type': 'keyword'}}}}
LOCAL = threading.local()


def source(i):
    return {'rank': i, 'group': f'g{i % 20:02d}', 'price': (i * 17) % 1000,
            'description': f'Product {i}: deterministic benchmark payload ' + 'x' * 160}


def request(base, path, method='POST', body=None, ndjson=False, allow404=False):
    if not hasattr(LOCAL, 'connections'):
        LOCAL.connections = {}
    if base not in LOCAL.connections:
        url = urllib.parse.urlsplit(base)
        cls = http.client.HTTPSConnection if url.scheme == 'https' else http.client.HTTPConnection
        LOCAL.connections[base] = cls(url.hostname, url.port, timeout=120)
    conn = LOCAL.connections[base]
    data = body if ndjson else (json.dumps(body, separators=(',', ':')) if body is not None else None)
    started = time.perf_counter()
    conn.request(method, path, body=data, headers={'Content-Type': 'application/x-ndjson' if ndjson else 'application/json'})
    response = conn.getresponse()
    raw = response.read()
    elapsed = (time.perf_counter() - started) * 1000
    if response.status >= 400 and not (response.status == 404 and allow404):
        raise RuntimeError(f'{base}{path}: {response.status}: {raw[:1500]!r}')
    return json.loads(raw), elapsed, len(raw)


def hits_expected(ids):
    return [(str(i), source(i)) for i in ids]


def check_hits(value, ids, total=None):
    actual = [(hit['_id'], hit['_source']) for hit in value['hits']['hits']]
    assert actual == hits_expected(ids), f'incorrect hits: {actual[:2]}, expected {ids[:2]}'
    if total is not None:
        assert value['hits']['total'] == {'value': total, 'relation': 'eq'}, value['hits']['total']


def operation(base, workload, i):
    if workload == 'point_read':
        key = (i * 7919) % N
        value, elapsed, size = request(base, f'/{INDEX}/_doc/{key}', 'GET')
        assert value['_source'] == source(key) and value['_id'] == str(key)
    elif workload == 'filtered_search':
        group = i % 20
        ids = list(range(group, N, 20))
        value, elapsed, size = request(base, f'/{INDEX}/_search', body={
            'query': {'term': {'group': f'g{group:02d}'}}, 'sort': [{'rank': 'asc'}], 'size': 20, 'track_total_hits': True})
        check_hits(value, ids[:20], len(ids))
    elif workload == 'sorted_pagination':
        start = (i * 97) % (N - 20)
        value, elapsed, size = request(base, f'/{INDEX}/_search', body={
            'query': {'match_all': {}}, 'sort': [{'rank': 'asc'}], 'search_after': [start - 1], 'size': 20, 'track_total_hits': True})
        check_hits(value, list(range(start, start + 20)), N)
    elif workload == 'terms_aggregation':
        value, elapsed, size = request(base, f'/{INDEX}/_search', body={
            'size': 0, 'aggs': {'groups': {'terms': {'field': 'group', 'size': 20}}}})
        actual = {bucket['key']: bucket['doc_count'] for bucket in value['aggregations']['groups']['buckets']}
        assert actual == {f'g{k:02d}': len(range(k, N, 20)) for k in range(20)}, actual
    elif workload == 'filter_aggregation':
        value, elapsed, size = request(base, f'/{INDEX}/_search', body={'size': 0, 'aggs': {
            'cheap': {'filter': {'range': {'price': {'lt': 250}}}},
            'group': {'filter': {'term': {'group': 'g03'}}}}})
        assert value['aggregations']['cheap']['doc_count'] == sum((j * 17) % 1000 < 250 for j in range(N))
        assert value['aggregations']['group']['doc_count'] == len(range(3, N, 20))
    else:
        raise ValueError(workload)
    return elapsed, size


def resources():
    try:
        result = subprocess.run(['docker', 'stats', '--no-stream', '--format', '{{json .}}'],
                                capture_output=True, text=True, timeout=20, check=True)
        return [json.loads(line) for line in result.stdout.splitlines() if line]
    except (OSError, subprocess.SubprocessError) as exc:
        return {'unavailable': str(exc)}


def save(result):
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(result, indent=2) + '\n')


def load(base):
    request(base, '/' + INDEX, 'DELETE', allow404=True)
    request(base, '/' + INDEX, 'PUT', MAPPING)
    started = time.perf_counter()
    for start in range(0, N, BATCH):
        lines = []
        for i in range(start, min(start + BATCH, N)):
            lines.extend((json.dumps({'index': {'_id': str(i)}}), json.dumps(source(i))))
        value, _, _ = request(base, f'/{INDEX}/_bulk?refresh=false', body='\n'.join(lines) + '\n', ndjson=True)
        assert not value.get('errors', True), str(value)[:2000]
    accepted = time.perf_counter() - started
    request(base, f'/{INDEX}/_refresh')
    polls = 0
    while True:
        count, _, _ = request(base, f'/{INDEX}/_count', body={'query': {'match_all': {}}})
        polls += 1
        if count['count'] == N:
            break
        if time.perf_counter() - started > accepted + 120:
            raise AssertionError(f'visibility timeout: {count}')
        time.sleep(.05)
    visible = time.perf_counter() - started
    return {'accepted_seconds': accepted, 'search_visible_seconds': visible,
            'visibility_count_polls': polls, 'refresh_and_visibility_seconds': visible - accepted,
            'accepted_documents_per_second': N / accepted, 'visible_documents_per_second': N / visible}


def main():
    assert N >= 100 and REQUESTS > 0 and TRIALS > 0 and BATCH > 0 and all(c > 0 for c in CONCURRENCY)
    rng = random.Random(20261015)
    result = {'created_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'config': {'documents': N, 'batch': BATCH, 'requests_per_trial': REQUESTS,
                         'trials': TRIALS, 'concurrency': CONCURRENCY, 'settle_seconds': SETTLE,
                         'seed': 20261015, 'mapping': MAPPING, 'persistent_http': True},
              'versions': {}, 'ingest': [], 'measurements': [], 'resources': []}
    try:
        for name, base in ENDPOINTS.items():
            result['versions'][name] = request(base, '/', 'GET')[0]
        for trial in range(TRIALS):
            names = list(ENDPOINTS)
            rng.shuffle(names)
            for name in names:
                timing = load(ENDPOINTS[name])
                result['ingest'].append({'engine': name, 'trial': trial, **timing})
                print('ingest', name, trial, timing, flush=True)
                save(result)
        time.sleep(SETTLE)
        workloads = ['point_read', 'filtered_search', 'sorted_pagination', 'terms_aggregation', 'filter_aggregation']
        for concurrency in CONCURRENCY:
            for workload in workloads:
                for trial in range(TRIALS):
                    names = list(ENDPOINTS)
                    rng.shuffle(names)
                    for name in names:
                        base = ENDPOINTS[name]
                        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
                            list(pool.map(lambda i: operation(base, workload, i), range(max(10, concurrency * 2))))
                            started = time.perf_counter()
                            samples = list(pool.map(lambda i: operation(base, workload, i), range(REQUESTS)))
                            wall = time.perf_counter() - started
                        latencies = sorted(x[0] for x in samples)
                        row = {'engine': name, 'workload': workload, 'concurrency': concurrency,
                               'trial': trial, 'requests': REQUESTS, 'wall_seconds': wall,
                               'throughput_rps': REQUESTS / wall, 'p50_ms': statistics.median(latencies),
                               'p95_ms': latencies[math.ceil(.95 * REQUESTS) - 1],
                               'latencies_ms': latencies, 'response_bytes': sum(x[1] for x in samples),
                               'all_responses_correct': True}
                        result['measurements'].append(row)
                        print(name, workload, concurrency, trial, round(row['p50_ms'], 3), flush=True)
                        save(result)
                result['resources'].append({'workload': workload, 'concurrency': concurrency, 'idle_after_trials': resources()})
        result['status'] = 'passed'
    except Exception as exc:
        result['status'] = 'failed'
        result['failure'] = str(exc)
        raise
    finally:
        save(result)


if __name__ == '__main__':
    main()
