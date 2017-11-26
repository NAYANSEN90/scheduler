package com.example.scheduler;


import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;


public class JedisJobStore implements IJobStore{


    private static String host = "127.0.0.1";
    private static int    port = 6379;
    private Jedis jedis = new Jedis(host, port);

    private static final String HASH_PREFIX = "JOB:";
    private static final String QUEUE_KEY   = "JOB_QUEUE";


    private AtomicBoolean scriptLoaded = new AtomicBoolean(false);

    private static final String fetchJobScript =
            "local queue = KEYS[1]\n" +
            "local now = ARGV[1]\n" +
            "local payload = nil\n" +
            "\n" +
            "local i, payload = next(redis.pcall('ZRANGEBYSCORE', queue, '-inf', now, 'LIMIT', '0' , '1'))\n" +
            "if payload then\n" +
            "   redis.pcall('ZREM', queue, payload)\n" +
            "end\n" +
            "return payload\n";

    private static String scriptSHA = "";


    public JedisJobStore(){
        loadLUA();
    }

    /* script loading needs to be done separately */
    public void loadLUA(){
        if(scriptLoaded.compareAndSet(false, true)) {
            scriptSHA = jedis.scriptLoad(fetchJobScript);
        }
    }

    public void flushLUA(){
        jedis.scriptFlush();
    }

    private static String generateID(){
        return UUID.randomUUID().toString();
    }

    private void addJob(JobDetail detail, String id, long timeOut){
        Transaction t = jedis.multi();
        t.hmset(HASH_PREFIX + id, detail.transform());
        t.zadd(QUEUE_KEY, timeOut, id);
        t.exec();
    }

    private void updateJobState(String id, String state){
        Transaction t = jedis.multi();
        t.hset(HASH_PREFIX + id, "state", state);
        t.exec();
    }

    private JobDetail formJobDetail(JobRequest.Request request, long delay){

        String id = generateID();
        JobRequest.Request request1 = JobRequest.Request.newBuilder(request).setId(id).build();
        JobDetail detail = new JobDetail(id, request1.toString(), "IDLE ", System.currentTimeMillis(), delay * 1000);

        return detail;
    }

    private String fetchJobId(){

        String jobId = (String)jedis.evalsha(scriptSHA,
                Arrays.asList(QUEUE_KEY),
                Arrays.asList(String.valueOf(System.currentTimeMillis())));

        return jobId;
    }


    @Override
    public synchronized void submitJobRequest(JobRequest.Request request){

        JobDetail detail = formJobDetail(request, 0);
        addJob(detail, detail.getId(), detail.getCreatedOn());
    }

    @Override
    public synchronized void submitDelayedJobRequest(JobRequest.Request request, int after){

        JobDetail detail = formJobDetail(request, after);
        addJob(detail, detail.getId(), detail.getCreatedOn() + (after * 1000));
    }

    @Override
    public synchronized JobDetail fetchTimedOutJob(){

        String jobId = fetchJobId();
        JobDetail detail = null;

        if(jobId != null && !jobId.isEmpty()){

            updateJobState(jobId, "RUNNING");
            detail = fetchJob(jobId);
        }

        return detail;
    }

    @Override
    public synchronized JobDetail fetchJob(String id){

        JobDetail detail = null;
        Transaction t1 = jedis.multi();
        Response<Map<String,String>>  resp = t1.hgetAll(HASH_PREFIX + id);
        t1.exec();

        if(resp.get() != null && !resp.get().isEmpty()){
            detail = new JobDetail(resp.get());
        }

        return detail;
    }

    @Override
    public synchronized void jobCompleted(String id, boolean success){
        String state = "FAILED";
        if(success) {
            state = "COMPLETED";
        }

        updateJobState(id, state);
    }

    @Override
    public synchronized void deleteJob(String id){
        Transaction t = jedis.multi();
        t.del(id);
        t.zrem(QUEUE_KEY, id);
        t.exec();
    }
}
