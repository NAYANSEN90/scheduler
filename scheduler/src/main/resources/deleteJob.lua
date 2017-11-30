---
--- Created by nayansen.
--- DateTime: 30/11/17 12:37 AM
---
local queueKey = KEYS[1]
local hashKey  = KEYS[2]

local fieldName  = ARGV[1]
local id = ARGV[2]
local state = "RUNNING"

local hashExists  = redis.call('EXISTS', hashKey)
local queueExists = redis.call('EXISTS', queueKey)

if hashExists == 1 then
    local payload = redis.call('HGET', hashKey, tostring(fieldName))
    if payload ~= state then
        -- delete from the hash key only if job is not running
        redis.call('DEL',hashKey)
    else
        return "0"
    end
end

if queueExists == 1 then
    redis.call('ZREM', queueKey,id)
end
return "1"