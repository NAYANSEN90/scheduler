package com.example.scheduler;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.util.Map;

public class JedisHashOperation implements IHashOperation {

    private String host = "127.0.0.1";
    private int port = 6379;
    private String prefix;

    private Jedis jedis = new Jedis(host, port) ;

    public JedisHashOperation(){

    }

    public void setHashPrefix(String prefix){
        this.prefix = prefix;
    }

    public synchronized void addHashed(JobDetail job, String id){
        Transaction t = jedis.multi();
        t.hmset(id, job.transform());
        t.exec();
    }

    public synchronized void removeHashed(String id){
        Transaction t = jedis.multi();
        t.del(id);
        t.exec();
    }

    public synchronized JobDetail fetchHash(String id){
        Transaction t = jedis.multi();
        Response<Map<String,String>> resp = t.hgetAll(id);
        t.exec();

        if(resp.get() == null){
            return null;
        }

        return new JobDetail( resp.get());
    }

    public synchronized void setHashField(String id, String field, String value){
        Transaction t = jedis.multi();
        t.hset(id, field, value);
        t.exec();
    }
}
