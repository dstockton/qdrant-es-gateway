#!/usr/bin/env python3
"""External launcher for the isolated backend RPC diagnostic; never changes engines."""
import argparse
import json
from pathlib import Path
import re
import subprocess
import urllib.parse

import measure
from snapshot_round import HERE, ROOT, command, snapshot
from thread_round import backend, pool, quiet_control

RUNNER = 'python@sha256:1b668429b3511ab407d8e00648891631b0b1a4d7e15e3ca70f38ab5b91ad4ab4'


def collection_state(collection):
    state = backend('/collections/' + urllib.parse.quote(collection, safe=''))
    sanitized = {k: state[k] for k in ('status', 'optimizer_status', 'points_count', 'segments_count')}
    if sanitized['status'] != 'green' or sanitized['optimizer_status'] != 'ok' or sanitized['points_count'] != 10000:
        raise RuntimeError('Prepared collection is not green with exactly10000points')
    return sanitized


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--prefix', default='backend-decomposition')
    args = parser.parse_args()
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,79}', args.prefix):
        parser.error('Use a short lowercase filename prefix')
    output = HERE / 'results' / (args.prefix + '.json')
    evidence_path = HERE / 'results' / (args.prefix + '-resources.json')
    if output.exists() or evidence_path.exists():
        raise SystemExit('Refusing to overwrite evidence')
    preparation = json.loads((HERE / 'results' / 'preparation.json').read_text())
    if preparation.get('status') != 'prepared' or preparation.get('documents') != 10000:
        raise RuntimeError('Requires existing prepared10000document corpus')
    index = measure.runner.benchmark.INDEX
    if index != 'balanced_benchmark':
        raise RuntimeError('Unexpected benchmark index')
    collection = 'es_' + index
    quiet_control()
    observed_pool = pool()
    if (observed_pool['high_cpu_threads'], observed_pool['high_io_threads']) != (2, 8):
        raise RuntimeError('Expected automatic Qdrant pools2/8')
    provenance = measure.provenance()
    expected = {'gateway': (.5, 512 * 1024**2), 'qdrant': (1.25, 3328 * 1024**2),
                'cache-valkey': (.25, 256 * 1024**2), 'elasticsearch': (2, 4 * 1024**3),
                'opensearch': (2, 4 * 1024**3)}
    for service, (cpu, memory) in expected.items():
        if (provenance[service]['cpu_limit'], provenance[service]['memory_limit_bytes']) != (cpu, memory):
            raise RuntimeError('Unexpected service resource limits')
    image_id = command('docker', 'image', 'inspect', RUNNER, '--format', '{{.Id}}').strip()
    evidence = {'diagnostic': 'backend-three-rpc-decomposition-not-native-equivalent',
                'provenance': provenance, 'runner': {'repo_digest': RUNNER, 'image_id': image_id,
                'cpu_limit': 2, 'memory_limit_bytes': 256 * 1024**2,
                'budget_note': 'Diagnostic load generator is outside compared server resource budgets.'},
                'collection_before': collection_state(collection), 'pool_before': observed_pool,
                'before': snapshot(), 'status': 'started'}
    with evidence_path.open('x') as handle:
        json.dump(evidence, handle, indent=2)
    try:
        subprocess.run(['docker', 'run', '--rm', '--network', 'qeg-shared-cache_default',
                        '--cpus', '2', '--memory', '256m', '--read-only', '--cap-drop', 'ALL',
                        '--security-opt', 'no-new-privileges', '--env', 'PYTHONDONTWRITEBYTECODE=1',
                        '--mount', f'type=bind,src={HERE / "backend_decomposition.py"},dst=/diagnostic.py,readonly',
                        '--mount', f'type=bind,src={HERE / "results"},dst=/results', RUNNER,
                        'python', '-u', '/diagnostic.py', '--collection', collection,
                        '--output', '/results/' + output.name], cwd=ROOT, check=True)
        result = json.loads(output.read_text())
        if result.get('status') != 'passed':
            raise RuntimeError('Diagnostic did not pass')
        evidence['status'] = 'passed'
    except Exception as error:
        evidence.update(status='failed', failure_type=type(error).__name__)
        raise
    finally:
        evidence.update(after=snapshot(), collection_after=collection_state(collection), pool_after=pool())
        evidence_path.write_text(json.dumps(evidence, indent=2) + '\n')


if __name__ == '__main__':
    main()
