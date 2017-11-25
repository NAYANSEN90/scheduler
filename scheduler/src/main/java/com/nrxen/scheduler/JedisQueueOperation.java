package com.nrxen.scheduler;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.util.Set;
import java.util.UUID;

public class JedisQueueOperation implements IQueueOperation {

    private String host = "127.0.0.1";
    private int port = 6379;

    private String queue;
    private Jedis jedis = new Jedis(host, port) ;

    public JedisQueueOperation(){

    }

    private String generateID(){
        return UUID.randomUUID().toString();
    }

    @Override
    public void setQueue(String name){
        this.queue = queue;
    }

    @Override
    public synchronized void enqueueNow(JobRequest.Request request) {
        String id = generateID();
        JobRequest.Request request1 = JobRequest.Request.newBuilder(request).setId(id).build();
        Transaction t = jedis.multi();
        Response<Long> out = t.zadd(queue.getBytes() , System.currentTimeMillis(),request1.toByteArray());
        t.exec();
    }

    @Override
    public synchronized void enqueueDelayed(JobRequest.Request request, int after) {
        String id = generateID();
        int time = after * 1000;
        JobRequest.Request request1 = JobRequest.Request.newBuilder(request).setId(id).build();
        Transaction t = jedis.multi();
        Response<Long> out = t.zadd(queue.getBytes() , System.currentTimeMillis() + time ,request1.toByteArray());
        t.exec();
    }

    public synchronized JobRequest.Request dequeueOne(){
        long currentTime = System.currentTimeMillis();
        Transaction t = jedis.multi();
        Response<Set<String>> resp = t.zrangeByScore(queue,0, currentTime,0, 1);
        t.exec();
        if(resp.get().isEmpty()){
            return null;
        }
        String reqStr = resp.get().toArray()[0].toString();
        try{
            JobRequest.Request request = JobRequest.Request.parseFrom(reqStr.getBytes());
            return request;
        }catch (Exception e){
            // This should never occur.
            System.out.println("Error while extracting job request from queue. Unrecoverable, job lost");
            return null;
        }
    }
}
