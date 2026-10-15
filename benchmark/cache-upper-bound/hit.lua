-- Synthetic warm-hit protocol only: no miss publication or write handling.
local control = redis.call('HMGET', KEYS[1], 'identity', 'epoch', 'writer_count')
if not control[1] or not control[2] or control[3] ~= '0' then return false end
if redis.call('EXISTS', KEYS[2]) == 0 then return false end
if redis.call('EXISTS', KEYS[3]) ~= 0 then return false end
local entry = redis.call('HMGET', KEYS[4], 'identity', 'epoch', 'payload')
if entry[1] ~= control[1] or entry[2] ~= control[2] then return false end
return entry[3]
