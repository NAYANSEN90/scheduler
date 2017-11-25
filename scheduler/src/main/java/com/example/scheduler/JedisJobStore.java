package com.example.scheduler;


import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class JedisJobStore implements IJobStore {

    private JedisQueueOperation queueOperation = new JedisQueueOperation();
    private JedisHashOperation  hashOperation  = new JedisHashOperation();
    private static String host = "127.0.0.1";
    private static int    port = 6379;
    private Jedis jedis = new Jedis(host, port);
    private static final String HASH_PREFIX = "JOB:";
    private static final String QUEUE_KEY   = "JOB_QUEUE";

    private String scriptSHA = "";
    private String script = "val = redis.call('zrangebyscore', KEYS[1], 0, KEYS[2], 0, 1)\n" +
                            "if val then\n" +
                            "       redis.call('hset', val,KEYS[3],KEYS[4])\n" +
                            "       redis.call('del', KEYS[1], val)\n" +
                            "       val1 = redis.call('hgetall',val)\n" +
                            "end\n" +
                            "return val1";

    private String testScript  = "val = redis.call('ping') return val";

    public JedisJobStore(){
        queueOperation.setQueue("JOB_QUEUE");
        hashOperation.setHashPrefix("JOB:");
    }

    public void loadLUAScript(){
        System.out.println(jedis.eval(testScript));
        scriptSHA = jedis.scriptLoad(script);
    }

    private String generateID(){
        return UUID.randomUUID().toString();
    }

    public synchronized void submitJobRequest(JobRequest.Request request){
        /* generate id */
        String id = generateID();
        JobRequest.Request request1 = JobRequest.Request.newBuilder(request).setId(id).build();
        JobDetail detail = new JobDetail(id, request1.toString(), "IDLE ", System.currentTimeMillis(),0);

        Transaction t = jedis.multi();
        t.hmset(HASH_PREFIX + id, detail.transform());
        t.zadd(QUEUE_KEY,System.currentTimeMillis(), HASH_PREFIX + id);
        t.exec();
    }

    public synchronized void submitDelayedJobRequest(JobRequest.Request request, int after){
        /* generate id */
        String id = generateID();
        JobRequest.Request request1 = JobRequest.Request.newBuilder(request).setId(id).build();
        JobDetail detail = new JobDetail(id, request1.toString(), "IDLE ", System.currentTimeMillis(), after * 1000);

        Transaction t = jedis.multi();
        t.hmset(HASH_PREFIX + id, detail.transform());
        t.zadd(QUEUE_KEY,System.currentTimeMillis() + after*1000, HASH_PREFIX + id);
        t.exec();
    }

    public synchronized JobDetail fetchJob(){
        /*
        String id = queueOperation.dequeueOne();
        queueOperation.delete(id);
        hashOperation.setHashField(id,"state","RUNNING");
        return  hashOperation.fetchHash(id);
        */
        List<String> keys = Arrays.asList(QUEUE_KEY,String.valueOf(System.currentTimeMillis()),"state","RUNNING");
        return  (JobDetail) jedis.eval(script,keys,Arrays.asList(""));
    }

    public synchronized void jobCompleted(String id, boolean success){
        String state = "FAILED";
        if(success) {
            state = "COMPLETED";
        }
        Transaction t = jedis.multi();
        t.hset(HASH_PREFIX + id, "state", state);
        t.exec();
    }

    public synchronized void deleteJob(String id){
        hashOperation.removeHashed(id);
    }
}
