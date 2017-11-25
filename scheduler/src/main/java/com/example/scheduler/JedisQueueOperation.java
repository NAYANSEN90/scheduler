package com.example.scheduler;


import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisTransaction;
import io.vertx.redis.op.RangeLimitOptions;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.util.Set;

public class JedisQueueOperation implements IQueueOperation {

    private String host = "127.0.0.1";
    private int port = 6379;

    private String queue;
    private Jedis jedis = new Jedis(host, port) ;

    public JedisQueueOperation(){

    }

    @Override
    public void setQueue(String name){
        this.queue = queue;
    }

    @Override
    public synchronized void enqueueNow(String jobId) {
        Transaction t = jedis.multi();
        Response<Long> out = t.zadd(queue, System.currentTimeMillis(), jobId);
        t.exec();
    }

    @Override
    public synchronized void enqueueDelayed(String jobId, int after) {
        int time = after * 1000;
        Transaction t = jedis.multi();
        Response<Long> out = t.zadd(queue, System.currentTimeMillis() + time , jobId);
        t.exec();
    }

    @Override
    public synchronized String dequeueOne(){
        long currentTime = System.currentTimeMillis();
        Transaction t = jedis.multi();
        Response<Set<String>> resp = t.zrangeByScore(queue,0, currentTime,0, 1);
        // Remove this current element
        t.exec();
        if(resp.get().isEmpty()){
            return "";
        }
        return resp.get().toArray()[0].toString();
        // utilize lua to perform this unique delete
    }

    public synchronized void delete(String id){
        Transaction t = jedis.multi();
        t.zrem(queue, id);
        t.exec();
    }
}
