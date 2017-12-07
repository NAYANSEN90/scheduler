local queued  = KEYS[1]
local running = KEYS[2]
local hashPrefix = KEYS[3]
local now = ARGV[1]
local payload = nil

local i, payload = next(redis.call('ZRANGEBYSCORE', queued, '-inf', now, 'LIMIT', '0' , '1'))
if payload then
    redis.call('ZREM', queued, payload)
    local hashKey = hashPrefix .. payload
    redis.call('HSET', hashKey, "state", "RUNNING")
    redis.call('ZADD', running, now, payload)
end
return payload
