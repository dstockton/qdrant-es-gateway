#!/usr/bin/env python3
"""Check the same maintenance workflow against two endpoints; no third-party packages."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import time

from replay import call

FIXTURE = Path(__file__).with_name('fixtures') / 'manufacturing.json'


def run(base, fixture, settle_seconds):
    index = fixture['index']
    records, checks = [], []

    def check(name, actual, expected):
        checks.append({'name': name, 'ok': actual == expected,
                       'actual': actual, 'expected': expected})

    def request(name, suffix='', method='POST', body=None, status=200, ndjson=False):
        path = '/' + index + suffix
        encoded = body if ndjson else (json.dumps(body) if body is not None else None)
        result = call(base, path, method, encoded,
                      'application/x-ndjson' if ndjson else 'application/json')
        # Deliberately omit timing: this is a correctness check, not a benchmark.
        result.pop('elapsed_ms', None)
        records.append({'name': name, 'method': method, 'path': path,
                        'request': body, 'response': result})
        check(name + ' HTTP status', result['status'], status)
        return result.get('body') or {}

    def search(name, expected):
        result = request(name, '/_search', body=fixture['search'])
        hits = result.get('hits', {}).get('hits', [])
        check(name + ' IDs', sorted(h.get('_id') for h in hits), sorted(expected))
        check(name + ' sources', {h.get('_id'): h.get('_source') for h in hits},
              {i: fixture['documents'][i] for i in expected})

    def facets(name, expected):
        result = request(name, '/_search', body=fixture['facet'])
        buckets = result.get('aggregations', {}).get('teams', {}).get('buckets', [])
        check(name + ' counts', {b['key']: b['doc_count'] for b in buckets}, expected)
        check(name + ' zero hits', result.get('hits', {}).get('hits'), [])

    created = request('create', method='PUT', body=fixture['mapping'])
    if created.get('acknowledged') is not True:
        return {'calls': records, 'checks': checks, 'aborted': 'Index not created; existing data left untouched'}
    try:
        bulk = ''.join(json.dumps({'index': {'_id': i}}) + '\n' + json.dumps(doc) + '\n'
                       for i, doc in fixture['documents'].items())
        result = request('bulk', '/_bulk?refresh=wait_for', body=bulk, ndjson=True)
        check('bulk errors', result.get('errors'), False)
        check('bulk items', [{k: item.get('index', {}).get(k) for k in ('_id', 'status', 'result')}
                             for item in result.get('items', [])],
              [{'_id': i, 'status': 201, 'result': 'created'} for i in fixture['documents']])
        time.sleep(settle_seconds)
        # Read every record so even documents excluded by search have verified sources.
        for i, source in fixture['documents'].items():
            result = request('read ' + i, '/_doc/' + i, method='GET')
            check('read ' + i + ' ID/source', [result.get('_id'), result.get('_source')], [i, source])
        search('open pump', fixture['expected_search_ids'])
        facets('open teams', fixture['expected_facets_before'])
        i = fixture['update_id']
        result = request('close job', '/_update/' + i + '?refresh=wait_for', body=fixture['update'])
        check('close job ID/result', [result.get('_id'), result.get('result')], [i, 'updated'])
        time.sleep(settle_seconds)
        result = request('read closed job', '/_doc/' + i, method='GET')
        expected = {**fixture['documents'][i], **fixture['update']['doc']}
        check('closed job preserves notes', [result.get('_id'), result.get('_source')], [i, expected])
        search('closed pump excluded', [])
        facets('remaining teams', fixture['expected_facets_after'])
    finally:
        request('cleanup', method='DELETE')
    return {'calls': records, 'checks': checks}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--native', required=True)
    parser.add_argument('--gateway', required=True)
    parser.add_argument('--settle-seconds', type=float, default=5)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    if args.settle_seconds < 0:
        parser.error('--settle-seconds must be nonnegative')
    fixture = json.loads(FIXTURE.read_text())
    report = {'run_at': datetime.now(timezone.utc).isoformat(),
              'gateway_revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
              'fixture': 'validation/fixtures/manufacturing.json',
              'settle_seconds_after_each_write_phase': args.settle_seconds}
    for side in ('native', 'gateway'):
        base = getattr(args, side)
        root = call(base, '/')
        report[side] = run(base, fixture, args.settle_seconds)
        report[side]['reported_version'] = (root.get('body') or {}).get('version', {}).get('number')
    # Assert identical application requests as well as each side's independent expectations.
    signature = lambda side: [(c['method'], c['path'], c['request']) for c in report[side]['calls']]
    report['identical_requests'] = signature('native') == signature('gateway')
    checks = [c for side in ('native', 'gateway') for c in report[side]['checks']]
    report['passed'] = sum(c['ok'] for c in checks)
    report['total'] = len(checks)
    report['ok'] = (report['identical_requests'] and all(c['ok'] for c in checks)
                    and not any('aborted' in report[side] for side in ('native', 'gateway')))
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({k: report[k] for k in ('ok', 'passed', 'total', 'identical_requests')}))
    for c in checks:
        if not c['ok']:
            print(json.dumps(c))
    return 0 if report['ok'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
