#!/usr/bin/env python3
"""Disposable-service fault test; leaves an uncertain writer for explicit recovery.

Requires CACHE_TEST_QDRANT_CONTAINER and CACHE_TEST_VALKEY_CONTAINER. Never point
these variables at shared or production services. Uses cache_live.py endpoints.
"""
import json
import os
import subprocess
import time
import cache_live as c

qdrant = os.environ['CACHE_TEST_QDRANT_CONTAINER']
valkey = os.environ['CACHE_TEST_VALKEY_CONTAINER']

def docker(*args):
    subprocess.run(['docker', *args], check=True, capture_output=True)

index = c.INDEX
source = {'brand':'stable','price':1}
c.mutation('PUT','/'+index,{'mappings':{'properties':{'brand':{'type':'keyword'},'price':{'type':'integer'}}}})
c.mutation('PUT',f'/{index}/_doc/a',source)
point_path=f'/{index}/_doc/a'
search_path=f'/{index}/_search'
query={'query':{'term':{'brand':'stable'}},'size':10}
point=c.request(c.A,'GET',point_path)
search=c.request(c.A,'POST',search_path,query)
# A's cold responses must be reusable by B without any upstream request.
docker('pause',qdrant)
try:
    assert c.request(c.B,'GET',point_path)==point
    second=c.request(c.B,'POST',search_path,query)
    search.pop('took'); second.pop('took'); assert second==search
    c.request(c.B,'GET',point_path+'?request_cache=false',expected=(502,))
    # Timeout is ambiguous, even if the server has not consumed this request.
    c.request(c.A,'PUT','/'+index+'_uncertain',{},expected=(502,))
    before=c.redis('HGETALL',c.KEY+':read-cache:control')
    assert sum(k.startswith(b'writer:') for k in before[::2])==1
    docker('restart',valkey)
    for attempt in range(50):
        try:
            if c.redis('PING')==b'PONG': break
        except (ConnectionError,OSError): pass
        time.sleep(.1)
    after=c.redis('HGETALL',c.KEY+':read-cache:control')
    assert dict(zip(before[::2],before[1::2]))==dict(zip(after[::2],after[1::2]))
    # Both old point and search entries remain fenced after Valkey restart.
    c.request(c.B,'GET',point_path,expected=(502,))
    c.request(c.B,'POST',search_path,query,expected=(502,))
finally:
    docker('unpause',qdrant)
print(json.dumps({'index':index,'cross_replica_hits_without_backend':True,
                  'explicit_bypass_reached_backend':True,'uncertain_token_survived_valkey_restart':True,
                  'old_entries_fenced_after_restart':True,'uncertain_index':index+'_uncertain','requires_explicit_reconciliation':True},indent=2))
