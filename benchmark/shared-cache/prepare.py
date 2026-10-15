#!/usr/bin/env python3
"""Prepare a fresh disposable deployment; refuses to overwrite preparation evidence."""
import datetime,json,os,subprocess,sys,time,uuid
from pathlib import Path
HERE=Path(__file__).resolve().parent
PROJECT='qeg-shared-cache'
OUTPUT=HERE/'results'/'preparation.json'
if OUTPUT.exists():raise SystemExit('Preparation evidence already exists; use a fresh project/volume deliberately.')
identity=str(uuid.uuid4())
env=dict(os.environ,CACHE_BENCH_DEPLOYMENT=identity)
env.pop('READ_CACHE_DEPLOYMENT',None)  # null in initialize.yml must really be unset
compose=['docker','compose','-p',PROJECT,'-f',str(HERE.parent/'three-engine'/'compose.yml'),'-f',str(HERE/'compose.yml')]
# Refuse any preexisting project containers or volumes: preparation must not delete user data.
for kind,filters in [('ps',['-a']),('volume',['ls'])]:
 command=['docker',kind,*filters,'--filter',f'label=com.docker.compose.project={PROJECT}','-q']
 if subprocess.check_output(command,text=True).strip():raise SystemExit('Project already exists; inspect it rather than recreating or deleting it.')
os.environ.update(ES_URL='http://127.0.0.1:19300',OS_URL='http://127.0.0.1:19301',GATEWAY_URL='http://127.0.0.1:19302')
sys.path.insert(0,str(HERE.parent/'three-engine'))
import run as benchmark
result={'created_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'deployment':identity,'project':PROJECT,'documents':benchmark.N,'validation_profile':benchmark.VALIDATION_PROFILE,'ingest':[]}
OUTPUT.parent.mkdir(parents=True,exist_ok=True)
def save():OUTPUT.write_text(json.dumps(result,indent=2)+'\n')
save()
try:
 subprocess.run(compose+['-f',str(HERE/'initialize.yml'),'up','-d','--no-build'],env=env,check=True)
 for base in benchmark.ENDPOINTS.values():
  deadline=time.monotonic()+180
  while True:
   try:benchmark.request(base,'/','GET');break
   except Exception:
    benchmark.LOCAL.connections={}
    if time.monotonic()>deadline:raise
    time.sleep(1)
 for engine,base in benchmark.ENDPOINTS.items():
  result['ingest'].append({'engine':engine,**benchmark.load(base)});save()
 subprocess.run(compose+['stop','gateway'],env=env,check=True)
 deadline=time.monotonic()+120
 while True:
  raw=subprocess.check_output(['docker','exec',f'{PROJECT}-cache-valkey-1','valkey-cli','--raw','INFO','persistence'],text=True)
  info=dict(line.strip().split(':',1) for line in raw.splitlines() if ':' in line and not line.startswith('#'))
  if all(info.get(key)==value for key,value in [('aof_enabled','1'),('aof_rewrite_in_progress','0'),('aof_rewrite_scheduled','0'),('aof_last_bgrewrite_status','ok'),('aof_last_write_status','ok')]):break
  if time.monotonic()>deadline:raise RuntimeError('AOF initialization did not complete')
  time.sleep(1)
 # Initialization is now false; only the explicit command creates cache control.
 subprocess.run(compose+['run','--rm','--no-deps','gateway','cache','bootstrap','--quiesced'],env=env,check=True)
 subprocess.run(compose+['up','-d','--no-deps','--no-build','gateway'],env=env,check=True)
 deadline=time.monotonic()+120
 while True:
  try:
   benchmark.LOCAL.connections={}
   benchmark.request(benchmark.ENDPOINTS['gateway'],'/readyz','GET')
   break
  except Exception:
   if time.monotonic()>deadline:raise
   time.sleep(1)
 result['status']='prepared'
except Exception as error:
 result['status']='failed';result['failure_type']=type(error).__name__;raise
finally:save()
