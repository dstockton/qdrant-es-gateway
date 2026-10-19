#!/usr/bin/env python3
"""Explicit synthetic probe participant; never report this as gateway performance."""
import json,os,subprocess,sys,urllib.request
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'three-engine'))
import compare_reads as runner
runner.OUTPUT=Path(os.getenv('OUTPUT',str(Path(__file__).parent/'results'/'probe-read-only.json')))
if runner.OUTPUT.exists():
    raise SystemExit('Refusing to overwrite existing evidence: '+str(runner.OUTPUT))
runner.benchmark.ENDPOINTS['cache_probe']=runner.benchmark.ENDPOINTS.pop('gateway')
original=runner.provenance
def provenance():
    result=original()
    result['cache_probe']=result.pop('gateway')
    result['cache_probe']['diagnostic']='synthetic-100-percent-hit-upper-bound'
    result['cache_probe']['read_transport']='synthetic_valkey_lua_hit'
    data=json.loads(subprocess.check_output(['docker','inspect','qeg-balanced-cache-probe-valkey-1'],text=True))[0]
    result['valkey']={'image':data['Config']['Image'],'image_id':data['Image'],'cpu_limit':data['HostConfig']['NanoCpus']/1e9,'memory_limit_bytes':data['HostConfig']['Memory']}
    assert sum(result[k]['cpu_limit'] for k in ('cache_probe','qdrant','valkey'))==2
    assert sum(result[k]['memory_limit_bytes'] for k in ('cache_probe','qdrant','valkey'))==4*1024**3
    return result
runner.provenance=provenance
root=json.load(urllib.request.urlopen(runner.benchmark.ENDPOINTS['cache_probe']+'/',timeout=5))
if root.get('diagnostic')!='synthetic-100-percent-hit-upper-bound':
    raise SystemExit('Endpoint is not the synthetic cache probe')
try:
    runner.main()
finally:
    if runner.OUTPUT.exists():
        result=json.loads(runner.OUTPUT.read_text())
        result['diagnostic']='synthetic-100-percent-hit-upper-bound'
        result['limitations']=['100% prepopulated immutable hits; not gateway performance','No cold misses, mutation, invalidation, recovery or realistic hit-rate costs','Qdrant stays within aggregate budget but receives no reads on probe hits']
        runner.OUTPUT.write_text(json.dumps(result,indent=2)+'\n')
