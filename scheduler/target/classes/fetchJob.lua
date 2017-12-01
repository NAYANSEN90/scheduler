local queue = KEYS[1]
local hashPrefix = KEYS[2]
local now = ARGV[1]
local payload = nil

local i, payload = next(redis.call('ZRANGEBYSCORE', queue, '-inf', now, 'LIMIT', '0' , '1'))
if payload then
    redis.call('ZREM', queue, payload)
    local hashKey = hashPrefix .. payload
    redis.call('HSET', hashKey, "state", "RUNNING")
end
return payload
