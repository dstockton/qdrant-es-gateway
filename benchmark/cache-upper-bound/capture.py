#!/usr/bin/env python3
"""Read-only public fixture capture; every response passes independent assertions."""
import importlib.util,json,os,subprocess
from pathlib import Path
spec=importlib.util.spec_from_file_location('benchmark',Path(__file__).resolve().parents[1]/'three-engine'/'run.py')
b=importlib.util.module_from_spec(spec);spec.loader.exec_module(b)
requests=int(os.getenv('REQUESTS','200'))
base=os.getenv('GATEWAY_URL','http://localhost:19202')
output=Path(os.getenv('OUTPUT',str(Path(__file__).parent/'results'/'fixtures.json')))
if output.exists():
    raise SystemExit('Refusing to overwrite existing fixtures: '+str(output))
root=b.request(base,'/','GET')[0]
if root.get('diagnostic') or root.get('version',{}).get('number')=='synthetic-probe':
    raise SystemExit('Capture requires retained production gateway, not diagnostic probe')
original=b.request
entries={}
def capture(base,path,method='POST',body=None,**kwargs):
    effective=body
    if b.TRACK_TOTAL_HITS=='false' and path.split('?',1)[0].endswith('/_search'):
        effective={**body,'track_total_hits':False}
    payload=json.dumps(effective,separators=(',',':')) if effective is not None else ''
    value,elapsed,size=original(base,path,method,body,**kwargs)
    entries[(method,path,payload)]={'method':method,'path':path,'request_body':payload,'response':value}
    return value,elapsed,size
b.request=capture
for total in ('true','false'):
    b.TRACK_TOTAL_HITS=total
    for workload in ('point_read','filtered_search','sorted_pagination','terms_aggregation','filter_aggregation'):
        for i in range(max(40,requests)):
            b.operation(base,workload,i)
image=json.loads(subprocess.check_output(['docker','inspect','qeg-balanced-gateway-1'],text=True))[0]['Image']
result={'diagnostic':'synthetic-100-percent-hit-upper-bound','source_image_id':image,'documents':b.N,'captured_requests_per_workload':requests,'total_modes':['true','false'],'all_capture_responses_correct':True,'entries':list(entries.values())}
output.parent.mkdir(parents=True,exist_ok=True)
output.write_text(json.dumps(result,separators=(',',':'))+'\n')
print(json.dumps({'unique_entries':len(entries),'bytes':output.stat().st_size,'all_capture_responses_correct':True}))
