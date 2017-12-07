---
--- Created by nayansen.
--- DateTime: 04/12/17 9:21 PM
---
local COUNTER = KEYS[1]
redis.call('INCR', COUNTER)
return tostring(redis.call('GET', COUNTER))