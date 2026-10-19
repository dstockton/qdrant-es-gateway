#!/usr/bin/env python3
"""Warm actual-gateway comparison; same image/coordination, serving toggled externally."""
import json,os,subprocess,sys
from pathlib import Path
HERE=Path(__file__).resolve().parent
os.environ.setdefault('ES_URL','http://127.0.0.1:19300')
os.environ.setdefault('OS_URL','http://127.0.0.1:19301')
os.environ.setdefault('GATEWAY_URL','http://127.0.0.1:19302')
sys.path.insert(0,str(HERE.parent/'three-engine'))
import compare_reads as runner
PROJECT='qeg-shared-cache'
EXPECTED_ENDPOINTS={'elasticsearch':'http://127.0.0.1:19300','opensearch':'http://127.0.0.1:19301','gateway':'http://127.0.0.1:19302'}
runner.OUTPUT=Path(os.getenv('OUTPUT',str(HERE/'results'/'warm.json')))

def inspect_service(service):
 data=json.loads(subprocess.check_output(['docker','inspect',f'{PROJECT}-{service}-1'],text=True))[0]
 env=dict(s.split('=',1) for s in data['Config']['Env'] if '=' in s)
 result={'image':data['Config']['Image'],'image_id':data['Image'],'cpu_limit':data['HostConfig']['NanoCpus']/1e9,'memory_limit_bytes':data['HostConfig']['Memory']}
 if service=='gateway':
  result.update(cache_coordination=bool(env.get('READ_CACHE_DEPLOYMENT')),cache_serving=env.get('READ_CACHE_SERVE'),cache_ttl_seconds=env.get('READ_CACHE_TTL_SECONDS'),cache_max_bytes=env.get('READ_CACHE_MAX_BYTES'))
 return result

def provenance():
 result={s:inspect_service(s) for s in ['elasticsearch','opensearch','gateway','qdrant','cache-valkey']}
 assert result['gateway']['cache_coordination'] and result['gateway']['cache_serving'] in ('true','false')
 assert sum(result[s]['cpu_limit'] for s in ['gateway','qdrant','cache-valkey'])==2
 assert sum(result[s]['memory_limit_bytes'] for s in ['gateway','qdrant','cache-valkey'])==4*1024**3
 for service in ['elasticsearch','opensearch']:
  assert result[service]['cpu_limit']==2 and result[service]['memory_limit_bytes']==4*1024**3
 return result
runner.provenance=provenance

def native_cache_counters():
 return {name:runner.benchmark.request(base,f'/{runner.benchmark.INDEX}/_stats/request_cache','GET')[0]['_all']['total']['request_cache'] for name,base in runner.benchmark.ENDPOINTS.items() if name!='gateway'}

def valkey_observations():
 def cli(*args):return subprocess.check_output(['docker','exec',f'{PROJECT}-cache-valkey-1','valkey-cli','--raw',*args],text=True)
 info=dict(line.strip().split(':',1) for line in cli('INFO','ALL').splitlines() if ':' in line and not line.startswith('#'))
 allowed=['used_memory','used_memory_peak','keyspace_hits','keyspace_misses','evicted_keys','expired_keys','aof_enabled','aof_last_write_status','aof_rewrite_in_progress','aof_rewrite_scheduled']
 return {'info':{key:info[key] for key in allowed if key in info},'durability_config':cli('CONFIG','GET','appendonly','appendfsync','no-appendfsync-on-rewrite','maxmemory','maxmemory-policy').splitlines()}

def main():
 if runner.OUTPUT.exists():raise SystemExit('Refusing to overwrite existing evidence')
 assert runner.benchmark.ENDPOINTS==EXPECTED_ENDPOINTS,'Endpoint/provenance mismatch: only this disposable project is supported'
 observed=provenance()
 # Prime every distinct measured request on every engine. The base runner's
 # 40 warmups alone do not cover all 200 point reads and cursor requests.
 for base in runner.benchmark.ENDPOINTS.values():
  for workload in runner.WORKLOADS:
   for i in range(runner.REQUESTS):runner.benchmark.operation(base,workload,i)
 before={'native_request_cache':native_cache_counters(),'valkey':valkey_observations()}
 try:runner.main()
 finally:
  if runner.OUTPUT.exists():
   result=json.loads(runner.OUTPUT.read_text());result['profile']='actual-gateway-warm-serving-toggle'
   result['config'].update(experiment='actual-shared-cache-paired-v1',cache_coordination=observed['gateway']['cache_coordination'],cache_serving=observed['gateway']['cache_serving'])
   result['observations_before']=before
   try:result['observations_after']={'native_request_cache':native_cache_counters(),'valkey':valkey_observations()}
   except Exception as error:result['observation_error_type']=type(error).__name__
   result['limitations']=['Warm repeated requests; evaluate separate cold and invalidation profiles before adoption.','Coordination remains enabled in serving-off baseline; this comparison does not estimate cost versus a gateway with no cache infrastructure.']
   runner.OUTPUT.write_text(json.dumps(result,indent=2)+'\n')

if __name__=='__main__':main()
