---
--- Created by nayansen.
--- DateTime: 30/11/17 12:37 AM
---
local queue = KEYS[1]
local hashKey = KEYS[2]
local id = ARGV[1]


local hashExists  = redis.call('EXISTS', hashKey)
if hashExists == 1 then
    local payload = redis.call('HGET', hashKey, "state")
    if payload ~= "RUNNING" then
        -- delete from the hash key only if job is not running
        redis.call('DEL',hashKey)
        return "1"
    else
        return "0"
    end
end

-- if a job is running, failed or completed, it will not exist in queue (guaranteed)
-- deletion from queue matters for idle jobs
local queueRank   = redis.call('ZRANK', queue, id)
if queueRank ~= nil then
    redis.call('ZREM', queue, id)
end

return "0"
