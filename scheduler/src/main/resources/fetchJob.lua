local queue = KEYS[1]
local now = ARGV[1]
local payload = nil

local i, payload = next(redis.pcall('ZRANGEBYSCORE', queue, '-inf', now, 'LIMIT', '0' , '1'))
if payload then
   redis.pcall('ZREM', queue, payload)
end
return payload

