#!/usr/bin/env python3
"""Capture opt-in gateway phase timelines; diagnostic only, no native comparison.

Requires an already deployed diagnostic image. Never replaces services or writes
benchmark documents. Cold keys must be fresh for this image; cached cold keys fail.
"""
import argparse
import concurrent.futures
import http.client
import json
import re
import threading
import time

import measure
from snapshot_round import snapshot, HERE
from thread_round import pool, quiet_control

LOCAL = threading.local()
LABELS = {'cache.lookup', 'cache.hit_decode', 'cache.hit', 'cache.miss', 'cache.bypass',
          'catalog.parse_read_view', 'catalog.read_view', 'search.plan', 'search.backend_join',
          'search.after_join', 'response.json_encode', 'cache.collect_body', 'cache.envelope_parse',
          'cache.envelope_encode', 'cache.publish', 'qdrant.count', 'qdrant.ordered',
          'qdrant.scroll', 'qdrant.other', 'qdrant.request_encode', 'qdrant.send_headers',
          'qdrant.receive_body', 'qdrant.response_parse'}


def request(start, bypass=False):
    if not hasattr(LOCAL, 'connection'):
        LOCAL.connection = http.client.HTTPConnection('127.0.0.1', 19302, timeout=30)
    body = json.dumps({'query': {'range': {'rank': {'gte': start}}},
                       'sort': [{'rank': 'asc'}], 'size': 20, 'track_total_hits': True}, separators=(',', ':'))
    path = '/balanced_benchmark/_search' + ('?request_cache=false' if bypass else '')
    began = time.perf_counter()
    LOCAL.connection.request('POST', path, body, {'Content-Type': 'application/json'})
    response = LOCAL.connection.getresponse()
    raw = response.read()
    wall_ms = (time.perf_counter() - began) * 1000
    assert response.status == 200
    trace_raw = response.getheader('x-qeg-diagnostic-timing')
    assert trace_raw is not None and len(trace_raw) <= 8192, 'Diagnostic header missing or oversized'
    trace = json.loads(trace_raw)
    assert trace['version'] == 1 and trace['dropped'] == 0 and len(trace['events']) <= 48
    ids = {event['id'] for event in trace['events']}
    assert [event['id'] for event in trace['events']] == list(range(len(trace['events'])))
    for event in trace['events']:
        assert event['name'] in LABELS and event['start_us'] >= 0 and event['duration_us'] >= 0
        assert event['parent'] is None or event['parent'] in ids
        assert event['start_us'] + event['duration_us'] <= trace['total_us'] + 1
        if event['parent'] is not None:
            parent = trace['events'][event['parent']]
            assert event['start_us'] >= parent['start_us']
            assert event['start_us'] + event['duration_us'] <= parent['start_us'] + parent['duration_us'] + 1
    measure.runner.benchmark.check_hits(json.loads(raw), list(range(start, start + 20)), 10000 - start)
    return {'start': start, 'wall_ms': wall_ms, 'response_bytes': len(raw),
            'diagnostic_header_bytes': len(trace_raw), 'trace': trace, 'all_responses_correct': True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image-id', required=True)
    parser.add_argument('--source', required=True)
    parser.add_argument('--prefix', default='gateway-phases')
    parser.add_argument('--start', type=int, default=1)
    parser.add_argument('--requests', type=int, default=100)
    args = parser.parse_args()
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,79}', args.prefix) or not re.fullmatch(r'sha256:[0-9a-f]{64}', args.image_id):
        parser.error('Require safe prefix and full image ID')
    if not 8 <= args.requests <= 200 or not 1 <= args.start or args.start + 6 * args.requests + 21 >= 10000:
        parser.error('Require requests8..200 and six disjoint ranges within10kcorpus')
    output = HERE / 'results' / (args.prefix + '.json')
    if output.exists():
        raise SystemExit('Refusing to overwrite evidence')
    quiet_control()
    provenance = measure.provenance()
    if provenance['gateway']['image_id'] != args.image_id or provenance['gateway']['cache_serving'] != 'true':
        raise RuntimeError('Requires selected diagnostic image with cache serving enabled')
    expected = {'gateway': (.5, 512 * 1024**2), 'qdrant': (1.25, 3328 * 1024**2),
                'cache-valkey': (.25, 256 * 1024**2)}
    for service, caps in expected.items():
        assert (provenance[service]['cpu_limit'], provenance[service]['memory_limit_bytes']) == caps
    observed_pool = pool()
    assert (observed_pool['high_cpu_threads'], observed_pool['high_io_threads']) == (2, 8)
    result = {'diagnostic': 'actual-gateway-phase-timeline-not-performance-comparison',
              'source': args.source, 'provenance': provenance, 'pool_before': observed_pool,
              'config': {'requests': args.requests, 'concurrency': [1, 8], 'start': args.start,
                         'worker_warmup': 'two uncached rank>=8000+worker requests and barrier before sampling'},
              'profiles': [], 'status': 'started',
              'limitations': ['Diagnostic instrumentation and headers add overhead; not a production latency benchmark.',
                             'total_us is dispatch scope: excludes HTTP body ingress, final diagnostic-header serialization and network delivery.',
                             'search.backend_join includes local response handling as well as overlapping backend RPCs.',
                             'Per-profile resource snapshots include untimed worker connection primers.',
                             'Count selectivity follows recorded start range; compare it explicitly before interpreting other experiments.',
                             'qdrant.send_headers includes pool/socket wait, backend execution and scheduling; receive_body is remaining response delivery.',
                             'response.json_encode covers es_ok serialization only; response Value construction is inside search.after_join.',
                             'Phase durations are wall intervals, not CPU cost; preserve per-request relationships instead of summing categories.',
                             'Intervals overlap; do not sum backend durations or subtract independently measured percentiles.',
                             'search.after_join includes fallback work on other request shapes; here the ordered fast path is required.']}
    with output.open('x') as handle:
        json.dump(result, handle, indent=2)
    try:
        for ci, concurrency in enumerate((1, 8)):
            for pi, profile in enumerate(('cold', 'warm', 'bypass')):
                offset = args.start + (ci * 3 + pi) * args.requests
                if profile == 'warm':
                    request(offset)
                before = snapshot()
                barrier = threading.Barrier(concurrency, timeout=30)
                def worker(worker_number):
                    try:
                        for _ in range(2):
                            request(8000 + worker_number, bypass=True)
                        barrier.wait()
                        return [request(offset if profile == 'warm' else offset + i, profile == 'bypass')
                                for i in range(worker_number, args.requests, concurrency)]
                    finally:
                        if hasattr(LOCAL, 'connection'):
                            LOCAL.connection.close()
                            del LOCAL.connection
                with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
                    samples = [sample for batch in executor.map(worker, range(concurrency)) for sample in batch]
                after = snapshot()
                result['profiles'].append({'profile': profile, 'concurrency': concurrency,
                                           'before': before, 'after': after, 'samples': samples})
                for sample in samples:
                    names = [event['name'] for event in sample['trace']['events']]
                    if profile == 'cold':
                        assert 'cache.miss' in names and 'cache.publish' in names and 'cache.hit' not in names
                    elif profile == 'warm':
                        assert 'cache.hit' in names and not any(name.startswith('qdrant.') for name in names)
                    else:
                        assert 'cache.bypass' in names and 'cache.publish' not in names
                    if profile != 'warm':
                        assert names.count('qdrant.count') == 1 and names.count('qdrant.ordered') == 1 and names.count('qdrant.scroll') == 1
                output.write_text(json.dumps(result, indent=2) + '\n')
                print('COMPLETED', profile, concurrency, flush=True)
        result['pool_after'] = pool()
        result['status'] = 'passed'
    except Exception as error:
        result.update(status='failed', failure_type=type(error).__name__)
        raise
    finally:
        output.write_text(json.dumps(result, indent=2) + '\n')


if __name__ == '__main__':
    main()
