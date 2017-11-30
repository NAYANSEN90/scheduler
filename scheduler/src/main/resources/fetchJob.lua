local queue = KEYS[1]
local now = ARGV[1]
local payload = nil

local i, payload = next(redis.call('ZRANGEBYSCORE', queue, '-inf', now, 'LIMIT', '0' , '1'))
if payload then
   redis.call('ZREM', queue, payload)
end
return payload

