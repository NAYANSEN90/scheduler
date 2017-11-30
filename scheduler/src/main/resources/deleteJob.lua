---
--- Created by nayansen.
--- DateTime: 30/11/17 12:37 AM
---
local queueKey = KEYS[1]
local hashKey  = KEYS[2]

local fieldName  = ARGV[1]
local fieldState = ARGV[2]

local hashExists  = next(redis.call('EXISTS ', hashKey))
local queueExists = next(redis.call('EXISTS', queueKey))

if hashExists == 1 then
    local i,payload = next(redis.call('HGET', hashKey, tostring(fieldName)))
    if payload ~= fieldState then
        -- delete from the hash key only if job is not running
        redis.call('DEL',hashKey)
    else
        return 0
    end
end

if queueExists == 1 then
    local i, payload = next(redis.call('ZREM', queueKey, queueKey))
end

return 1