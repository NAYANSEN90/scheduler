val = redis.call('zrangebyscore', KEYS[1], 0, KEYS[2], 0, 1)
if val then
       redis.call('hset', val,KEYS[4],KEYS[5])
       redis.call('del', KEYS[1], val)
       val1 = redis.call('hgetall',val)
end
return val1