#!/usr/bin/env python3
"""Run one pinned-image round of the atomic cache/catalog snapshot experiment.

Run on-main old1,new1,new2,old2, then off-control in the same ABBA order.
No images are built. Existing evidence is never overwritten. Resource snapshots
are outside profile timing; counters cover setup/priming as well as requests.
"""
import argparse
import datetime
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
SERVICES = ('gateway', 'qdrant', 'cache-valkey', 'elasticsearch', 'opensearch')
BASELINE = 'sha256:220649fc71be02210342fef90f9b8d2426cfb8013ee8285ce44cc8c41aa61fc4'


def command(*args):
    return subprocess.check_output(args, text=True, cwd=ROOT)


def snapshot():
    result = {'captured_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'cpu': {}, 'scope': 'outside timers; includes profile setup and priming'}
    for service in SERVICES:
        name = f'qeg-shared-cache-{service}-1'
        info = json.loads(command('docker', 'inspect', name))[0]
        counters = command('docker', 'exec', name, 'cat', '/sys/fs/cgroup/cpu.stat')
        result['cpu'][service] = {
            'container_id': info['Id'], 'image_id': info['Image'],
            'counters': {k: int(v) for k, v in (line.split() for line in counters.splitlines())}}
    raw = command('docker', 'exec', 'qeg-shared-cache-cache-valkey-1',
                  'valkey-cli', '--raw', 'INFO', 'ALL')
    info = dict(line.strip().split(':', 1) for line in raw.splitlines()
                if ':' in line and not line.startswith('#'))
    fields = ('used_memory', 'used_memory_peak', 'keyspace_hits', 'keyspace_misses',
              'evicted_keys', 'expired_keys', 'aof_current_size', 'aof_rewrites',
              'aof_rewrite_in_progress', 'aof_rewrite_scheduled', 'aof_delayed_fsync',
              'aof_last_write_status', 'aof_last_bgrewrite_status')
    result['valkey'] = {k: info.get(k) for k in fields}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('kind', choices=('on-main', 'off-control'))
    parser.add_argument('version', choices=('old', 'new'))
    parser.add_argument('round', choices=('1', '2'))
    parser.add_argument('--prefix', default='atomic-snapshot')
    parser.add_argument('--old-image', default='qdrant-es-gateway:catalog-transport')
    parser.add_argument('--old-image-id', default=BASELINE)
    parser.add_argument('--old-source', default='49da1cb')
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
    preparation = json.loads((HERE / 'results' / 'preparation.json').read_text())
    image = args.old_image if args.version == 'old' else args.new_image
    expected = args.old_image_id if args.version == 'old' else args.new_image_id
    env = dict(os.environ, CACHE_BENCH_DEPLOYMENT=preparation['deployment'],
               CACHE_BENCH_SERVE=str(serving).lower(), SNAPSHOT_IMAGE=image)
    evidence = {'experiment': 'atomic-cache-catalog-snapshot-v1', 'arguments': vars(args),
                'profiles': {}, 'status': 'started'}
    resources.parent.mkdir(parents=True, exist_ok=True)
    with resources.open('x') as handle:
        json.dump(evidence, handle, indent=2)
    try:
        subprocess.run(['docker', 'compose', '-p', 'qeg-shared-cache',
                        '-f', 'benchmark/three-engine/compose.yml',
                        '-f', 'benchmark/shared-cache/compose.yml',
                        '-f', 'benchmark/shared-cache/snapshot-image.yml',
                        'up', '-d', '--no-build', '--no-deps', 'gateway'],
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
        for profile, output in outputs.items():
            before = snapshot()
            if before['cpu']['gateway']['image_id'] != expected:
                raise RuntimeError('Running image identity mismatch')
            evidence['profiles'][profile] = {'before': before}
            trial_env = env | {'OUTPUT': str(output), 'REQUESTS': '200' if serving else '100',
                               'TRIALS': '3', 'WARMUP': '40', 'ROUND': name + '-' + profile,
                               'PROFILE': profile, 'TRACK_TOTAL_HITS': 'true',
                               'CONCURRENCY': '1,8', 'DOCS': '10000', 'REQUEST_CACHE': 'default'}
            try:
                subprocess.run([sys.executable, str(HERE / ('measure.py' if profile == 'warm' else 'mixed.py'))],
                               cwd=ROOT, env=trial_env, check=True)
            finally:
                evidence['profiles'][profile]['after'] = snapshot()
            result = json.loads(output.read_text())
            if result.get('status') != 'passed' or result['provenance']['gateway']['image_id'] != expected:
                raise RuntimeError('Benchmark verification failed; evidence retained')
            result['config'].update(snapshot_experiment=evidence['experiment'], gateway_version=args.version)
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
