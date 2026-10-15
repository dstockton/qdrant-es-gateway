#!/usr/bin/env python3
"""Exercise two coordinated gateways against disposable Qdrant/Valkey services.

Set GATEWAY_A/GATEWAY_B and CACHE_TEST_REDIS_PORT; bootstrap their shared catalog
and cache control first. This creates/deletes only a UUID-named test index.
Run with projection enabled on both replicas to cover source/search mutations.
No timing/performance claims are made by this correctness suite.
"""
import json
import os
import socket
import urllib.error
import urllib.request
import uuid

A = os.environ.get('GATEWAY_A', 'http://127.0.0.1:19203')
B = os.environ.get('GATEWAY_B', 'http://127.0.0.1:19204')
PORT = int(os.environ.get('CACHE_TEST_REDIS_PORT', '16380'))
KEY = os.environ.get('CACHE_TEST_CATALOG_KEY', 'qeg:{cache-live}:v1')
INDEX = 'cache_live_' + uuid.uuid4().hex
checks = []


def redis(*args):
    encoded = [str(a).encode() for a in args]
    wire = b'*%d\r\n' % len(encoded) + b''.join(b'$%d\r\n' % len(a) + a + b'\r\n' for a in encoded)
    with socket.create_connection(('127.0.0.1', PORT), timeout=5) as sock:
        sock.sendall(wire)
        f = sock.makefile('rb')
        def read():
            line = f.readline()
            if not line: raise ConnectionError('Valkey connection closed')
            typ, value = line[:1], line[1:-2]
            if typ == b'-': raise AssertionError(value.decode())
            if typ == b':': return int(value)
            if typ == b'+': return value
            if typ == b'$':
                n = int(value)
                if n == -1: return None
                data = f.read(n); assert f.read(2) == b'\r\n'; return data
            if typ == b'*': return [read() for _ in range(int(value))]
            raise AssertionError(line)
        return read()


def request(base, method, path, body=None, expected=(200, 201)):
    payload = body.encode() if isinstance(body, str) else None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(base + path, payload, {'Content-Type':'application/json'}, method=method)
    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            status, raw = response.status, response.read()
    except urllib.error.HTTPError as error:
        status, raw = error.code, error.read()
    value = json.loads(raw) if raw else None
    assert status in expected, (method,path,status,value)
    return value


def mutation(method, path, body=None, expected=(200,201), base=B):
    before = int(redis('HGET', KEY + ':read-cache:control', 'epoch'))
    value = request(base,method,path,body,expected)
    after = int(redis('HGET', KEY + ':read-cache:control', 'epoch'))
    assert after > before, (method,path,'epoch did not advance')
    fields = redis('HKEYS', KEY + ':read-cache:control')
    assert not any(f.startswith(b'writer:') for f in fields), (method,path,'writer stranded')
    checks.append(method + ' ' + path.replace(INDEX,'<index>'))
    return value


def point(id, expected):
    first=request(A,'GET',f'/{INDEX}/_doc/{id}')
    second=request(B,'GET',f'/{INDEX}/_doc/{id}')
    assert first['_source'] == second['_source'] == expected
    entries=redis('KEYS',KEY+':read-cache:entry:*')
    assert entries, 'cold reads did not publish'


def search():
    body={'query':{'match_all':{}},'sort':[{'price':'asc'}],'size':20,'aggs':{'brands':{'terms':{'field':'brand'}}}}
    first=request(A,'POST',f'/{INDEX}/_search',body)
    second=request(B,'POST',f'/{INDEX}/_search',body)
    first.pop('took'); second.pop('took'); assert first == second
    uncached=request(B,'POST',f'/{INDEX}/_search?request_cache=false',body)
    uncached.pop('took'); assert first == uncached


def main():
    try:
        mutation('PUT','/'+INDEX,{'mappings':{'properties':{'brand':{'type':'keyword'},'price':{'type':'integer'}}}})
        source={'brand':'A','price':1,'large':18446744073709551615,'small':-9223372036854775808}
        mutation('PUT',f'/{INDEX}/_doc/a',source); point('a',source); search()
        source['price']=2
        mutation('POST',f'/{INDEX}/_update/a',{'doc':{'price':2}}); point('a',source); search()
        noop=mutation('POST',f'/{INDEX}/_update/a',{'doc':{'price':2}}); assert noop['result']=='noop'
        mutation('PUT',f'/{INDEX}/_create/a',source,expected=(409,))
        mutation('POST',f'/{INDEX}/_create/b',{'brand':'B','price':3})
        generated=mutation('POST',f'/{INDEX}/_doc',{'brand':'C','price':4})['_id']
        mutation('PUT',f'/{INDEX}/_mapping',{'properties':{'extra':{'type':'keyword'}}})
        alias=INDEX+'_alias'
        mutation('POST','/_aliases',{'actions':[{'add':{'index':INDEX,'alias':alias}}]})
        assert request(A,'GET',f'/{alias}/_doc/a')['_source']==source
        for number,(method,path) in enumerate([('POST','/_bulk'),('PUT','/_bulk'),('POST',f'/{INDEX}/_bulk'),('PUT',f'/{INDEX}/_bulk')]):
            body=json.dumps({'index':{'_index':INDEX,'_id':f'bulk{number}'}})+'\n'+json.dumps({'brand':'bulk','price':10+number})+'\n'+json.dumps({'update':{'_index':INDEX,'_id':'absent'}})+'\n'+json.dumps({'doc':{'price':5}})+'\n'
            result=mutation(method,path,body); assert result['errors'] is True
            point(f'bulk{number}',{'brand':'bulk','price':10+number}); search()
        mutation('DELETE',f'/{INDEX}/_doc/{generated}')
        request(A,'GET',f'/{INDEX}/_doc/{generated}',expected=(404,)); search()
        mutation('POST','/_aliases',{'actions':[{'remove':{'index':INDEX,'alias':alias}}]})
        request(A,'GET',f'/{alias}/_doc/a',expected=(404,))
        # Over-limit responses remain intact instead of being truncated for caching.
        large={'brand':'large','price':99,'blob':'x'*(1100*1024)}
        mutation('PUT',f'/{INDEX}/_doc/large',large)
        assert request(A,'GET',f'/{INDEX}/_doc/large')['_source']==large
        assert request(B,'GET',f'/{INDEX}/_doc/large')['_source']==large
        print(json.dumps({'passed':len(checks),'mutations':checks,'projection':True,'performance_claim':False},indent=2))
    finally:
        mutation('DELETE','/'+INDEX)


if __name__ == "__main__":
    main()
