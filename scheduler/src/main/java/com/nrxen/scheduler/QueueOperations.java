package com.nrxen.scheduler;

import io.vertx.redis.RedisClient;

public class QueueOperations {

    private RedisClient client;

    public QueueOperations(RedisClient client){
        this.client = client;
    }

    public synchronized boolean addNow(){

        return false;
    }

    public synchronized boolean addDelayed(){
        return false;
    }
}
