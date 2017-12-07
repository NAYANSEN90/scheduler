---
--- Created by nayansen.
--- DateTime: 30/11/17 12:37 AM
---
local queued  = KEYS[1]
local success = KEYS[2]
local failed  = KEYS[3]
local hashKey = KEYS[4]
local id = ARGV[1]

-- lack of score for a key indicates the key doesn't exist
-- a job id will exist in only one set ( if running state of job, all these states don't matter)
local hashExists  = redis.call('EXISTS', hashKey)
if hashExists == 1 then
    local payload = redis.call('HGET', hashKey, "state")
    if payload ~= "RUNNING" then
        redis.call('DEL',hashKey)
        local queueName = "UNKNOWN";
        if payload == "SUCCEEDED" then
            queueName = success;
        elseif payload == "FAILED" then
            queueName = failed
        elseif payload == "QUEUED" then
            queueName = queued
        end
        local score = redis.call('ZSCORE', queueName, id)
        if score ~= nil then
            redis.call('ZREM', queueName, id)
        end
    end
    return "1"
else
    return "0"
end


