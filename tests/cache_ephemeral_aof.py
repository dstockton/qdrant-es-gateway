#!/usr/bin/env python3
"""Verify production cache Lua against an isolated durable Valkey, including crashes.

No host ports or existing data are used. Successful runs remove only their own
container/volume; failed runs retain them stopped for inspection. Output is JSON.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]


def command(*args):
    return subprocess.check_output(args, text=True, stderr=subprocess.PIPE).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image', default='valkey/valkey@sha256:081c2f5cb575efc901aa80ff9cdbd1ec6a301682fd35e1ebb4b0990a4a4a8507')
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise SystemExit('Refusing to overwrite evidence')
    source = (ROOT / 'src/read_cache.rs').read_text()
    catalog_source = (ROOT / 'src/shared_catalog.rs').read_text()
    scripts = dict(re.findall(r'const (\w+): &str = r#"(.*?)"#;', source, re.S))
    assert all(k in scripts for k in ('VALIDATE', 'LOOKUP', 'PUBLISH', 'ADMIT', 'RELEASE'))
    assert 'set_repl' in scripts['PUBLISH'], 'Candidate source required'
    begin = re.search(r'redis::Script::new\(\s*r#"(.*?)"#', catalog_source[catalog_source.index('async fn begin_snapshot'):], re.S)[1]
    commit = re.search(r'redis::Script::new\(\s*r#"(.*?)"#', catalog_source[catalog_source.index('async fn publish'):], re.S)[1]
    name = 'qeg-ephemeral-proof-' + uuid.uuid4().hex[:12]
    volume = name + '-data'
    cat = 'qeg:{ephemeral-proof}:v1'
    control, admin, entry = cat + ':read-cache:control', cat + ':admin', cat + ':read-cache:entry:test'
    deployment, semantic = 'proof-v1', 'proof-build'
    result = {'status': 'started', 'source_sha256': hashlib.sha256(source.encode()).hexdigest(),
              'catalog_source_sha256': hashlib.sha256(catalog_source.encode()).hexdigest(),
              'checks': [], 'scope': 'isolated production Lua crash/rewrite proof; not a benchmark'}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    created = False

    def cli(*parts, error=False):
        raw = command('docker', 'exec', name, 'valkey-cli', '--json', *map(str, parts))
        if raw.startswith('error:'):
            if error:
                return raw
            raise AssertionError(raw)
        value = json.loads(raw)
        if error:
            raise AssertionError('Expected command error, got ' + repr(value))
        return value

    def info():
        raw = command('docker', 'exec', name, 'valkey-cli', '--raw', 'INFO', 'ALL')
        return dict(line.split(':', 1) for line in raw.splitlines()
                    if ':' in line and not line.startswith('#'))

    def ready():
        for _ in range(100):
            try:
                if cli('PING') == 'PONG' and info()['aof_rewrite_in_progress'] == '0':
                    return
            except (subprocess.CalledProcessError, ValueError, AssertionError):
                pass
            time.sleep(.1)
        raise AssertionError('Valkey did not become ready')

    def crash():
        command('docker', 'kill', '--signal=KILL', name)
        command('docker', 'start', name)
        ready()

    def run(operation, *values, error=False):
        lua = 'local allow_maintenance = ' + ('true' if operation == 'RELEASE' else 'false') + '\n'
        lua += scripts['VALIDATE'] + '\n' + scripts[operation]
        return cli('EVAL', lua, 4, control, cat, admin, entry,
                   '1', deployment, *values, error=error)

    def publish(epoch, ttl=600, error=False):
        return run('PUBLISH', semantic, epoch, '{"found":true,"_id":"one"}', 1024, ttl, error=error)

    def lookup():
        return run('LOOKUP', semantic, 1024, 8 * 1024 * 1024)

    def admit(token):
        return run('ADMIT', token, 'active')[1]

    def release(token, epoch):
        assert run('RELEASE', token, epoch) == ['ok']

    def rewrite():
        before = int(info()['aof_rewrites'])
        cli('BGREWRITEAOF')
        for _ in range(200):
            observed = info()
            if int(observed['aof_rewrites']) > before and observed['aof_rewrite_in_progress'] == '0':
                assert observed['aof_last_bgrewrite_status'] == 'ok'
                return
            time.sleep(.05)
        raise AssertionError('Rewrite did not complete')

    try:
        command('docker', 'volume', 'create', volume)
        command('docker', 'run', '-d', '--name', name, '--network', 'none',
                '--mount', f'type=volume,src={volume},dst=/data', args.image,
                'valkey-server', '--save', '', '--appendonly', 'yes', '--appendfsync', 'always',
                '--auto-aof-rewrite-percentage', '0', '--maxmemory', '64mb', '--maxmemory-policy', 'noeviction')
        created = True
        ready()
        observed = info()
        version = observed.get('valkey_version', observed.get('redis_version'))
        assert version == '8.1.10', f'Expected Valkey8.1.10, got {version}'
        result['version'] = version
        result['image_id'] = json.loads(command('docker', 'inspect', name))[0]['Image']
        result['durability'] = cli('CONFIG', 'GET', 'appendonly', 'appendfsync', 'maxmemory-policy', 'save')
        initial_catalog = '{"indices":[],"aliases":[]}'
        cli('SET', cat, initial_catalog)
        cli('HSET', control, 'schema', '1', 'deployment', deployment, 'epoch', '0', 'state', 'active')
        before = int(info()['aof_current_size'])
        assert publish('0') == ['ok'] and lookup()[0] == 'hit'
        after = int(info()['aof_current_size'])
        assert after == before, (before, after)
        result['checks'].append({'name': 'publish_hits_without_incremental_aof_growth', 'before_bytes': before, 'after_bytes': after})
        # Match the gateway's fencing order: writer, then catalog claim.
        token = uuid.uuid4().hex
        epoch = admit(token)
        assert epoch == '1'
        claim = uuid.uuid4().hex
        assert cli('EVAL', begin, 2, cat, admin, claim, 8 * 1024 * 1024) == ['ok', initial_catalog]
        crash()
        assert cli('GET', admin) == claim and cli('GET', cat) == initial_catalog
        assert cli('HGET', control, 'writer:' + token) == epoch
        assert cli('EXISTS', entry) == 0 and lookup() == ['busy']
        result['checks'].append({'name': 'unresolved_admin_and_writer_survive_crash_cache_fill_does_not', 'passed': True})
        updated_catalog = '{"indices":[["books","{}","[]"]],"aliases":[]}'
        assert cli('EVAL', commit, 2, cat, admin, claim, updated_catalog) == 1
        release(token, epoch)
        crash()
        assert cli('GET', cat) == updated_catalog and cli('EXISTS', admin) == 0
        assert cli('HGET', control, 'writer:' + token) is None
        assert lookup()[0] == 'miss'
        result['checks'].append({'name': 'catalog_commit_and_claim_releases_survive_crash', 'passed': True})
        # Rewrite is allowed to snapshot cache entries; it must preserve expiry.
        assert publish(epoch) == ['ok']
        expiry = cli('PEXPIRETIME', entry)
        rewrite()
        crash()
        assert lookup()[0] == 'hit' and cli('PEXPIRETIME', entry) == expiry
        assert 0 < cli('PTTL', entry) <= 600000
        result['checks'].append({'name': 'rewrite_restores_same_epoch_hit_with_original_absolute_expiry', 'passed': True})
        token = uuid.uuid4().hex
        epoch = admit(token)
        release(token, epoch)
        crash()
        assert cli('EXISTS', entry) == 1, 'Old entry retained by rewrite fixture'
        assert lookup()[0] == 'miss', 'Durable epoch must reject restored old cache entry'
        assert cli('HGET', control, 'epoch') == epoch
        assert cli('HGET', control, 'writer:' + token) is None
        result['checks'].append({'name': 'durable_epoch_and_release_survive_crash_rejecting_rewritten_old_entry', 'passed': True})
        # Invalid TTL deliberately reaches SET after suppression, unlike validated API calls.
        error = publish(epoch, ttl=0, error=True)
        assert 'expire' in error.lower(), error
        token = uuid.uuid4().hex
        epoch = admit(token)
        crash()
        assert cli('HGET', control, 'writer:' + token) == epoch and lookup() == ['busy']
        release(token, epoch)
        result['checks'].append({'name': 'publication_error_cannot_suppress_later_writer_durability', 'passed': True})
        result['status'] = 'passed'
    except Exception as exc:
        result.update(status='failed', failure_type=type(exc).__name__, failure=str(exc), retained_container=name, retained_volume=volume)
        raise
    finally:
        args.output.write_text(json.dumps(result, indent=2) + '\n')
        if created:
            if result['status'] == 'passed':
                command('docker', 'rm', '-f', name)
                command('docker', 'volume', 'rm', volume)
            else:
                subprocess.run(['docker', 'stop', name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
