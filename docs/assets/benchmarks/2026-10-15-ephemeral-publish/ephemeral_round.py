#!/usr/bin/env python3
"""Run one pinned-image round of the ephemeral response-publication experiment.

Run on-main old1,new1,new2,old2, then off-control in the same ABBA order.
No images are built. Existing evidence is never overwritten. Resource snapshots
are outside profile timing; counters cover setup/priming as well as requests.
"""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
HERE = ROOT / 'benchmark' / 'shared-cache'
BASELINE = 'sha256:1c79aae1383e1190f83172b46fb1a9b7b352a3b131ea7fd3d20767af5bba2e20'


from snapshot_round import command, snapshot
from thread_round import pool, quiet_control


def verify_pool():
    observed = pool()
    if (observed['high_cpu_threads'], observed['high_io_threads']) != (2, 8):
        raise RuntimeError('Qdrant automatic pools must remain 2/8')
    return observed


def prime(measure):
    b = measure.runner.benchmark
    for base in b.ENDPOINTS.values():
        for workload in measure.runner.WORKLOADS:
            for i in range(200):
                b.operation(base, workload, i)
        for start in range(7000, 7200):
            value, _, _ = b.request(base, f'/{b.INDEX}/_search?request_cache=false', body={
                'query': {'range': {'rank': {'gte': start}}},
                'sort': [{'rank': 'asc'}], 'size': 20, 'track_total_hits': True})
            b.check_hits(value, list(range(start, start + 20)), b.N - start)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('kind', choices=('on-main', 'off-control'))
    parser.add_argument('version', choices=('old', 'new'))
    parser.add_argument('round', choices=('1', '2'))
    parser.add_argument('--prefix', default='ephemeral-publish')
    parser.add_argument('--old-image', default='qdrant-es-gateway:typed-readview')
    parser.add_argument('--old-image-id', default=BASELINE)
    parser.add_argument('--old-source', default='04b6d7eecce6230634109ec1905509666f00b8bc')
    parser.add_argument('--new-image', required=True)
    parser.add_argument('--new-image-id', required=True)
    parser.add_argument('--new-source', required=True)
    args = parser.parse_args()
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,79}', args.prefix):
        parser.error('prefix must be a short lowercase filename component')
    for digest in (args.old_image_id, args.new_image_id):
        if not re.fullmatch(r'sha256:[0-9a-f]{64}', digest):
            parser.error('expected image IDs must be full sha256 digests')
    if args.old_image_id == args.new_image_id:
        parser.error('baseline and candidate must be distinct images')
    serving = args.kind == 'on-main'
    profiles = ['cold', 'warm'] if serving else ['cold']
    name = f'{args.prefix}-{args.kind}-{args.version}-{args.round}'
    outputs = {p: HERE / 'results' / f'{name}-{p}.json' for p in profiles}
    resources = HERE / 'results' / f'{name}-resources.json'
    if any(p.exists() for p in [*outputs.values(), resources]):
        raise SystemExit('Refusing to overwrite retained evidence; no runtime change made')
    # Validate BOTH builds before changing the runtime.
    for image, expected in ((args.old_image, args.old_image_id),
                            (args.new_image, args.new_image_id)):
        if command('docker', 'image', 'inspect', image, '--format', '{{.Id}}').strip() != expected:
            raise SystemExit('Image identity mismatch; no runtime change made')
    quiet_control()
    initial_pool = verify_pool()
    preparation = json.loads((HERE / 'results' / 'preparation.json').read_text())
    image = args.old_image if args.version == 'old' else args.new_image
    expected = args.old_image_id if args.version == 'old' else args.new_image_id
    env = dict(os.environ, CACHE_BENCH_DEPLOYMENT=preparation['deployment'],
               CACHE_BENCH_SERVE=str(serving).lower(), EPHEMERAL_IMAGE=image)
    evidence = {'experiment': 'ephemeral-response-publish-paired-v1', 'arguments': vars(args),
                'profiles': {}, 'status': 'started', 'initial_pool': initial_pool,
                'primer': 'All engines: five workloads i0..199 and uncached sorted rank7000..7199, before each profile outside timers'}
    resources.parent.mkdir(parents=True, exist_ok=True)
    with resources.open('x') as handle:
        json.dump(evidence, handle, indent=2)
    try:
        subprocess.run(['docker', 'compose', '-p', 'qeg-shared-cache',
                        '-f', 'benchmark/three-engine/compose.yml',
                        '-f', 'benchmark/shared-cache/compose.yml',
                        '-f', 'benchmark/shared-cache/ephemeral-image.yml',
                        'up', '-d', '--no-build', '--no-deps', '--force-recreate', 'gateway'],
                       cwd=ROOT, env=env, check=True)
        for _ in range(60):
            try:
                with urllib.request.urlopen('http://127.0.0.1:19302/readyz', timeout=2) as response:
                    response.read()
                break
            except (urllib.error.URLError, TimeoutError, OSError):
                time.sleep(.5)
        else:
            raise RuntimeError('Gateway readiness failed')
        os.environ.update(ES_URL='http://127.0.0.1:19300', OS_URL='http://127.0.0.1:19301',
                          GATEWAY_URL='http://127.0.0.1:19302', DOCS='10000', TRACK_TOTAL_HITS='true',
                          CONCURRENCY='1,8', REQUEST_CACHE='default')
        import measure
        provenance = measure.provenance()
        if provenance['gateway']['image_id'] != expected:
            raise RuntimeError('Gateway image mismatch before priming')
        evidence['provenance'] = provenance
        for profile, output in outputs.items():
            verify_pool()
            prime(measure)
            before = snapshot()
            if before['cpu']['gateway']['image_id'] != expected:
                raise RuntimeError('Running image identity mismatch')
            evidence['profiles'][profile] = {'before': before, 'pool_before': verify_pool()}
            trial_env = env | {'OUTPUT': str(output), 'REQUESTS': '200' if serving else '100',
                               'TRIALS': '3', 'WARMUP': '40', 'ROUND': name + '-' + profile,
                               'PROFILE': profile, 'TRACK_TOTAL_HITS': 'true',
                               'CONCURRENCY': '1,8', 'DOCS': '10000', 'REQUEST_CACHE': 'default',
                               'ES_URL': 'http://127.0.0.1:19300', 'OS_URL': 'http://127.0.0.1:19301',
                               'GATEWAY_URL': 'http://127.0.0.1:19302'}
            try:
                subprocess.run([sys.executable, str(HERE / ('measure.py' if profile == 'warm' else 'mixed.py'))],
                               cwd=ROOT, env=trial_env, check=True)
            finally:
                evidence['profiles'][profile]['after'] = snapshot()
                evidence['profiles'][profile]['pool_after'] = verify_pool()
            result = json.loads(output.read_text())
            if result.get('status') != 'passed' or result['provenance']['gateway']['image_id'] != expected:
                raise RuntimeError('Benchmark verification failed; evidence retained')
            result['config'].update(ephemeral_publish_experiment=evidence['experiment'], gateway_version=args.version)
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
