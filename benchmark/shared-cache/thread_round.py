#!/usr/bin/env python3
"""One fixed-image Qdrant thread-limit ABBA round: auto1, one1, one2, auto2.

Force-recreate Qdrant and gateway each round, retaining volumes. Prime all engines
identically before each profile. Never build, initialize, or reset control state.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import urllib.error
import urllib.request

import snapshot_round as common

ROOT, HERE = common.ROOT, common.HERE
IMAGE = 'qdrant-es-gateway:atomic-snapshot'
DIGEST = 'sha256:1faf2724e09951316f7bd7699b9fb3199b302675627ce8683505156f3eb8139c'
SOURCE = '2deda5e43b72de9b2c2ad6f9043df2a01217fd55'


def backend(path):
    return json.loads(common.command('docker', 'exec', 'qeg-shared-cache-elasticsearch-1',
                                    'curl', '--max-time', '3', '-fsS', 'http://qdrant:6333' + path))['result']


def pool():
    value = backend('/telemetry?details_level=3')['search_pool']
    return {k: value[k] for k in ('mode', 'high_cpu_threads', 'high_io_threads')}


def quiet_control():
    # Required control hash must have only its four persistent metadata fields.
    script = "local k=KEYS[1]; if redis.call('TYPE',k).ok~='hash' or redis.call('HLEN',k)~=4 or redis.call('HGET',k,'state')~='active' or redis.call('EXISTS',KEYS[2])~=0 then return 0 end; return 1"
    control = 'qeg:{shared-cache-benchmark}:v1:read-cache:control'
    result = common.command('docker', 'exec', 'qeg-shared-cache-cache-valkey-1',
                            'valkey-cli', '--raw', 'EVAL', script, '2', control, 'qeg:{shared-cache-benchmark}:v1:admin').strip()
    if result != '1':
        raise RuntimeError('Control is not active and writer-free; no runtime change made')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('variant', choices=('auto', 'one'))
    parser.add_argument('round', choices=('1', '2'))
    args = parser.parse_args()
    name = f'threads-{args.variant}-{args.round}'
    outputs = {p: HERE / 'results' / f'{name}-{p}.json' for p in ('cold', 'warm')}
    resources = HERE / 'results' / f'{name}-resources.json'
    if any(p.exists() for p in [*outputs.values(), resources]):
        raise SystemExit('Refusing to overwrite evidence')
    if common.command('docker', 'image', 'inspect', IMAGE, '--format', '{{.Id}}').strip() != DIGEST:
        raise SystemExit('Pinned image mismatch')
    quiet_control()
    preparation = json.loads((HERE / 'results' / 'preparation.json').read_text())
    env = dict(os.environ, CACHE_BENCH_DEPLOYMENT=preparation['deployment'],
               CACHE_BENCH_SERVE='true', THREAD_GATEWAY_IMAGE=IMAGE,
               QDRANT_BENCH_SEARCH_THREADS='0' if args.variant == 'auto' else '1')
    evidence = {'experiment': 'qdrant-search-threads-v1', 'variant': args.variant,
                'gateway_image_id': DIGEST, 'gateway_source': SOURCE,
                'primer': 'All engines: every workload i=0..199 then uncached sorted rank>=7000..7199; before each profile, outside timers',
                'profiles': {}, 'status': 'started'}
    with resources.open('x') as handle:
        json.dump(evidence, handle, indent=2)
    try:
        compose = ['docker', 'compose', '-p', 'qeg-shared-cache',
                   '-f', 'benchmark/three-engine/compose.yml',
                   '-f', 'benchmark/shared-cache/compose.yml',
                   '-f', 'benchmark/shared-cache/thread-image.yml']
        # Quiesce the gateway before backend restart, then start it only after
        # Qdrant is ready: gateway startup performs live backend validation.
        subprocess.run(compose + ['stop', 'gateway'], cwd=ROOT, env=env, check=True)
        subprocess.run(compose + ['up', '-d', '--no-build', '--no-deps',
                                 '--force-recreate', 'qdrant'], cwd=ROOT, env=env, check=True)
        for _ in range(120):
            try:
                collections = backend('/collections')['collections']
                states = [backend('/collections/' + c['name']) for c in collections]
                if not states or not all(c['status'] == 'green' and c['optimizer_status'] == 'ok' for c in states):
                    raise RuntimeError('Collections not ready')
                break
            except (subprocess.CalledProcessError, RuntimeError, OSError):
                time.sleep(.5)
        else:
            raise RuntimeError('Backend readiness failed')
        subprocess.run(compose + ['up', '-d', '--no-build', '--no-deps',
                                 '--force-recreate', 'gateway'], cwd=ROOT, env=env, check=True)
        for _ in range(120):
            try:
                with urllib.request.urlopen('http://127.0.0.1:19302/readyz', timeout=2) as response:
                    response.read()
                break
            except (urllib.error.URLError, OSError):
                time.sleep(.5)
        else:
            raise RuntimeError('Gateway readiness failed')
        evidence['collection_shapes_after_restart'] = sorted(
            [{k: c.get(k) for k in ('status', 'optimizer_status', 'points_count', 'segments_count')} for c in states],
            key=lambda c: (c['points_count'], c['segments_count']))
        expected_pool = (2, 8) if args.variant == 'auto' else (1, 1)
        evidence['pool_after_restart'] = pool()
        if tuple(evidence['pool_after_restart'][k] for k in ('high_cpu_threads', 'high_io_threads')) != expected_pool:
            raise RuntimeError('Effective search pools differ from expected configuration')
        # Import only after fixed endpoint and corpus configuration is established.
        os.environ.update(ES_URL='http://127.0.0.1:19300', OS_URL='http://127.0.0.1:19301',
                          GATEWAY_URL='http://127.0.0.1:19302', DOCS='10000', TRACK_TOTAL_HITS='true',
                          CONCURRENCY='1,8', REQUEST_CACHE='default')
        import measure
        observed = measure.provenance()
        if observed['gateway']['image_id'] != DIGEST:
            raise RuntimeError('Running gateway digest mismatch')
        evidence['provenance'] = observed
        for profile, output in outputs.items():
            for base in measure.runner.benchmark.ENDPOINTS.values():
                for workload in measure.runner.WORKLOADS:
                    for i in range(200):
                        measure.runner.benchmark.operation(base, workload, i)
                for i in range(6999, 7199):
                    start = i + 1
                    b = measure.runner.benchmark
                    value, _, _ = b.request(base, f'/{b.INDEX}/_search?request_cache=false', body={
                        'query': {'range': {'rank': {'gte': start}}},
                        'sort': [{'rank': 'asc'}], 'size': 20, 'track_total_hits': True})
                    b.check_hits(value, list(range(start, start + 20)), b.N - start)
            record = {'before': common.snapshot(), 'pool_before': pool()}
            evidence['profiles'][profile] = record
            trial_env = env | {'OUTPUT': str(output), 'REQUESTS': '200', 'TRIALS': '3', 'WARMUP': '40',
                               'ROUND': name + '-' + profile, 'PROFILE': profile, 'TRACK_TOTAL_HITS': 'true',
                               'CONCURRENCY': '1,8', 'DOCS': '10000', 'REQUEST_CACHE': 'default',
                               'ES_URL': 'http://127.0.0.1:19300', 'OS_URL': 'http://127.0.0.1:19301',
                               'GATEWAY_URL': 'http://127.0.0.1:19302'}
            try:
                subprocess.run([sys.executable, str(HERE / ('measure.py' if profile == 'warm' else 'mixed.py'))],
                               cwd=ROOT, env=trial_env, check=True)
            finally:
                record.update(after=common.snapshot(), pool_after=pool())
            result = json.loads(output.read_text())
            if result.get('status') != 'passed' or result['provenance']['gateway']['image_id'] != DIGEST:
                raise RuntimeError('Profile verification failed')
            result['config'].update(thread_experiment=evidence['experiment'], search_threads=args.variant)
            output.write_text(json.dumps(result, indent=2) + '\n')
            print('COMPLETED', name, profile, flush=True)
        evidence['status'] = 'passed'
    except Exception as error:
        evidence.update(status='failed', failure_type=type(error).__name__)
        raise
    finally:
        resources.write_text(json.dumps(evidence, indent=2) + '\n')


if __name__ == '__main__':
    main()
