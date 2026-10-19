#!/usr/bin/env python3
"""Read-only Qdrant RPC diagnostic, NOT an Elasticsearch-compatible benchmark.

Run inside an existing Python container on qeg-shared-cache_default, mounting this
file read-only and only the results directory writable. No collection mutations.
Caller must pin/record container image and backend resource provenance separately.
"""
import argparse
import concurrent.futures
import datetime
import http.client
import json
import math
from pathlib import Path
import random
import statistics
import time
import threading
import urllib.parse

KINDS = ('count', 'coverage', 'ordered', 'parallel')


def bodies(start):
    scope = {'must': [{'key': 'rank', 'range': {'gte': start}}]}
    return {
        'count': {'exact': True, 'filter': scope},
        'coverage': {'filter': {'must_not': [{'key': '_qeg_fast_fields', 'match': {'value': 'v1:number:rank'}}],
                                'must': [scope]}, 'limit': 1, 'with_payload': False, 'with_vector': False},
        'ordered': {'limit': 21, 'order_by': {'key': 'rank', 'direction': 'asc'},
                    'with_payload': {'include': ['_es_id', '_source']}, 'with_vector': False,
                    'filter': {'must': [scope]}},
    }


def source(i):
    return {'rank': i, 'group': f'g{i % 20:02d}', 'price': (i * 17) % 1000,
            'description': f'Product {i}: deterministic benchmark payload ' + 'x' * 160}


def validate(kind, value, start):
    assert value['status'] == 'ok'
    elapsed = value['time']
    assert isinstance(elapsed, (int, float)) and math.isfinite(elapsed) and elapsed >= 0
    if kind == 'count':
        assert value['result']['count'] == 10000 - start
    elif kind == 'coverage':
        assert value['result']['points'] == []
        assert value['result']['next_page_offset'] is None
    else:
        points = value['result']['points']
        assert len(points) == 21
        for point, i in zip(points, range(start, start + 21)):
            assert point['payload']['_es_id'] == str(i)
            assert point['payload']['_source'] == source(i)


class Worker:
    def __init__(self, host, port, collection):
        self.connections = {k: http.client.HTTPConnection(host, port, timeout=30) for k in KINDS[:3]}
        self.collection = urllib.parse.quote(collection, safe='')
        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=3)

    def close(self):
        self.pool.shutdown(wait=True)
        for connection in self.connections.values():
            connection.close()

    def call(self, kind, start):
        body = json.dumps(bodies(start)[kind], separators=(',', ':')).encode()
        path = f'/collections/{self.collection}/points/' + ('count' if kind == 'count' else 'scroll')
        connection = self.connections[kind]
        began = time.perf_counter()
        connection.request('POST', path, body=body, headers={'Content-Type': 'application/json'})
        response = connection.getresponse()
        raw = response.read()
        wall_ms = (time.perf_counter() - began) * 1000
        assert response.status == 200, (kind, response.status)
        value = json.loads(raw)
        validate(kind, value, start)
        return {'rpc': kind, 'wall_ms': wall_ms, 'qdrant_response_time_ms': value['time'] * 1000,
                'response_bytes': len(raw)}

    def group(self, kind, start):
        began = time.perf_counter()
        if kind == 'parallel':
            futures = [self.pool.submit(self.call, k, start) for k in KINDS[:3]]
            calls = [f.result() for f in futures]
        else:
            calls = [self.call(kind, start)]
        return {'start': start, 'group_wall_ms': (time.perf_counter() - began) * 1000,
                'calls': calls, 'all_responses_correct': True}


def telemetry(host, port):
    connection = http.client.HTTPConnection(host, port, timeout=10)
    try:
        connection.request('GET', '/telemetry?details_level=3')
        response = connection.getresponse()
        value = json.loads(response.read())
        assert response.status == 200
        pool = value['result']['search_pool']
        return {k: pool[k] for k in ('mode', 'high_cpu_threads', 'high_io_threads')}
    finally:
        connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='qdrant')
    parser.add_argument('--port', type=int, default=6333)
    parser.add_argument('--collection', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--requests', type=int, default=100)
    parser.add_argument('--trials', type=int, default=4)
    parser.add_argument('--warmup-per-worker', type=int, default=10)
    args = parser.parse_args()
    if args.requests < 8 or args.requests > 1000 or args.trials < 4 or args.trials % 4 or args.trials * args.requests + 21 >= 10000 or not 1 <= args.warmup_per_worker <= 1000:
        parser.error('Require requests8..1000, trials positive multiple of4, fitting10kcorpus, positivewarmup')
    result = {'diagnostic': 'backend-three-rpc-decomposition-not-native-equivalent',
              'created_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'config': {'requests': args.requests, 'trials': args.trials, 'concurrency': [1, 8],
                         'warmup_per_worker': args.warmup_per_worker, 'seed': 20261015,
                         'documents': 10000, 'persistent_connection_per_worker_per_rpc': True},
              'request_bodies_at_start_1': bodies(1), 'measurements': [], 'status': 'started',
              'limitations': ['No gateway, catalog, cache, native engine, or public client path measured.',
                              'Parallel group wall includes dispatch, JSON parsing and correctness validation; per-call wall ends at response body receipt.',
                              'Do not sum parallel RPC timings or subtract independently measured quantiles.',
                              'Resource snapshots and container provenance must be supplied by the external harness.']}
    with args.output.open('x') as handle:
        json.dump(result, handle, indent=2)
    def save():
        args.output.write_text(json.dumps(result, indent=2) + '\n')
    try:
        result['pool_before'] = telemetry(args.host, args.port)
        rng = random.Random(20261015)
        for concurrency in (1, 8):
            order = list(KINDS)
            rng.shuffle(order)
            for trial in range(args.trials):
                trial_order = order[trial % 4:] + order[:trial % 4]
                for kind in trial_order:
                    barrier = threading.Barrier(concurrency, timeout=30)
                    def run_worker(worker_number):
                        worker = Worker(args.host, args.port, args.collection)
                        try:
                            for i in range(args.warmup_per_worker):
                                worker.group(kind, 7000 + i)
                            barrier.wait()
                            return [worker.group(kind, 1 + trial * args.requests + i)
                                    for i in range(worker_number, args.requests, concurrency)]
                        finally:
                            worker.close()
                    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
                        samples = [sample for group in pool.map(run_worker, range(concurrency)) for sample in group]
                    samples.sort(key=lambda sample: sample['start'])
                    timings = sorted(sample['group_wall_ms'] for sample in samples)
                    row = {'kind': kind, 'trial': trial, 'concurrency': concurrency, 'order': trial_order,
                           'samples': samples, 'p50_group_ms': statistics.median(timings),
                           'p95_group_ms': timings[math.ceil(.95 * len(timings)) - 1]}
                    result['measurements'].append(row)
                    save()
                    print(kind, concurrency, trial, round(row['p50_group_ms'], 3), flush=True)
        result['pool_after'] = telemetry(args.host, args.port)
        result['status'] = 'passed'
    except Exception as error:
        result.update(status='failed', failure_type=type(error).__name__)
        raise
    finally:
        save()


if __name__ == '__main__':
    main()
