#!/usr/bin/env python3
"""Separate cold-query and durable-write/invalidation experiments; never warm metrics."""
import concurrent.futures,datetime,json,math,os,random,statistics,time
from pathlib import Path
import measure
b=measure.runner.benchmark
PROFILE=os.getenv('PROFILE','cold')
REQUESTS=int(os.getenv('REQUESTS','200'))
TRIALS=int(os.getenv('TRIALS','3'))
OUTPUT=Path(os.getenv('OUTPUT',str(measure.HERE/'results'/f'{PROFILE}.json')))

def put_source(base,source,refresh=True):
 path=f'/{b.INDEX}/_doc/0'+('?refresh=true' if refresh else '')
 value,latency,size=b.request(base,path,'PUT',source)
 assert value.get('result') in ('created','updated'),value
 return latency,size

def read_filters(base,price):
 value,latency,size=b.request(base,f'/{b.INDEX}/_search',body={'size':0,'aggs':{
  'cheap':{'filter':{'range':{'price':{'lt':250}}}},'group':{'filter':{'term':{'group':'g03'}}}}})
 assert value['hits']['hits']==[]
 b.check_total(value,b.N)
 assert value['aggregations']['cheap']['doc_count']==b.EXPECTED_CHEAP_COUNT-(price>=250)
 assert value['aggregations']['group']['doc_count']==len(range(3,b.N,20))
 return latency,size

def cold(base,i):
 start=i+1
 value,latency,size=b.request(base,f'/{b.INDEX}/_search',body={
  'query':{'range':{'rank':{'gte':start}}},'sort':[{'rank':'asc'}],'size':20,'track_total_hits':True})
 b.check_hits(value,list(range(start,start+20)),b.N-start)
 return latency,size

def invalidation(base,i,kind):
 source=b.source(0);source['price']=999 if i%2==0 else 0
 write_ms,write_bytes=put_source(base,source,refresh=kind!='write_then_point_read')
 if kind=='write_then_point_read':
  value,read_ms,read_bytes=b.request(base,f'/{b.INDEX}/_doc/0','GET')
  assert value['_id']=='0' and value['_source']==source
 else:read_ms,read_bytes=read_filters(base,source['price'])
 return write_ms+read_ms,write_bytes+read_bytes,write_ms,read_ms

def main():
 assert PROFILE in ('cold','invalidation') and REQUESTS>0 and TRIALS>0
 assert b.ENDPOINTS==measure.EXPECTED_ENDPOINTS,'Only disposable shared-cache ports allowed for mutation profiles'
 assert REQUESTS*TRIALS*len(b.CONCURRENCY)+20<b.N,'Unique cold queries must fit corpus'
 if OUTPUT.exists():raise SystemExit('Refusing to overwrite existing evidence')
 result={'created_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'profile':PROFILE,
  'config':{'documents':b.N,'requests_per_trial':REQUESTS,'trials':TRIALS,'track_total_hits':b.TRACK_TOTAL_HITS,'validation_profile':b.VALIDATION_PROFILE,'refresh_contract':{'write_then_point_read':'omitted: realtime GET','write_then_filter_counts':'true: immediate search visibility','cold_setup_and_restoration':'true: outside read timing'},'read_only':False,'native_request_cache':'default'},
  'provenance':measure.provenance(),'observations_before':{'native_request_cache':measure.native_cache_counters(),'valkey':measure.valkey_observations()},'measurements':[]}
 result['config'].update(experiment='actual-shared-cache-'+PROFILE+'-v1',cache_coordination=result['provenance']['gateway']['cache_coordination'],cache_serving=result['provenance']['gateway']['cache_serving'])
 OUTPUT.parent.mkdir(parents=True,exist_ok=True)
 def save():OUTPUT.write_text(json.dumps(result,indent=2)+'\n')
 rng=random.Random(20261015)
 workloads=['unique_sorted_filter'] if PROFILE=='cold' else ['write_then_point_read','write_then_filter_counts']
 concurrency_values=b.CONCURRENCY if PROFILE=='cold' else [1]
 try:
  for c_index,concurrency in enumerate(concurrency_values):
   for workload in workloads:
    for trial in range(TRIALS):
     engines=list(b.ENDPOINTS);rng.shuffle(engines)
     for engine in engines:
      base=b.ENDPOINTS[engine]
      # Same visible write for every engine clears prior generation/cache state.
      # It is excluded only from cold-read latency, never the invalidation cycles.
      put_source(base,b.source(0))
      if PROFILE=='invalidation':
       if workload=='write_then_point_read':
        value,_,_=b.request(base,f'/{b.INDEX}/_doc/0','GET');assert value['_source']==b.source(0)
       else:read_filters(base,0)
      started=time.perf_counter()
      if PROFILE=='cold':
       offset=(c_index*TRIALS+trial)*REQUESTS
       with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        samples=list(pool.map(lambda i:cold(base,i),range(offset,offset+REQUESTS)))
      else:samples=[invalidation(base,i,workload) for i in range(REQUESTS)]
      wall=time.perf_counter()-started;values=sorted(x[0] for x in samples)
      row={'engine':engine,'workload':workload,'concurrency':concurrency,'trial':trial,'requests':REQUESTS,'all_responses_correct':True,'p50_ms':statistics.median(values),'p95_ms':values[math.ceil(.95*len(values))-1],'latencies_ms':values,'throughput_rps':REQUESTS/wall,'wall_seconds':wall,'response_bytes':sum(x[1] for x in samples)}
      if PROFILE=='invalidation':row.update(write_latencies_ms=[x[2] for x in samples],read_latencies_ms=[x[3] for x in samples])
      result['measurements'].append(row);save()
      print(engine,workload,concurrency,trial,round(row['p50_ms'],3),flush=True)
  result['status']='passed'
 except Exception as error:
  result['status']='failed';result['failure_type']=type(error).__name__;raise
 finally:
  for engine,base in b.ENDPOINTS.items():
   try:put_source(base,b.source(0))
   except Exception as error:result.setdefault('restoration_errors',{})[engine]=type(error).__name__;result['status']='failed'
  try:result['observations_after']={'native_request_cache':measure.native_cache_counters(),'valkey':measure.valkey_observations()}
  except Exception as error:result['observation_error_type']=type(error).__name__
  save()

if __name__=='__main__':main()
