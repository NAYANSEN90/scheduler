package com.example.scheduler;


import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;


public class JedisJobStore implements IJobStore{

    private static Logger logger = LoggerFactory.getLogger("JedisJobStore");

    private String host;
    private int    port;

    private Jedis jedis;

    private String HASH_PREFIX;
    private String QUEUE_KEY;


    private AtomicBoolean scriptLoaded = new AtomicBoolean(false);

    private String fetchJobLUA  = ResourceUtil.fetchLUA(Constants.FETCH_JOB_LUA);
    private String deleteJobLUA = ResourceUtil.fetchLUA(Constants.DELETE_JOB_LUA);

    private String fetchJobSHA;
    private String deleteJobSHA;

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

    private String scriptSHA = "";


    private static boolean testJedisConnection(final String host, final int port){
        boolean success = false;
        do {

            if(host == null || host.isEmpty()){
                break;
            }

            if(port <= 0){
                break;
            }
            Jedis tryJedis = new Jedis(host, port);
            String pong = "";
            try {
                pong = tryJedis.ping();
            }catch (Exception e){
                logger.error(e);
                break;
            }
            if(!pong.equals("pong")){
                break;
            }
            success = true;
        }while(false);

        return success;
    }

    public JedisJobStore() throws IllegalArgumentException{

        if(!testJedisConnection(Constants.DEFAULT_HOST, Constants.DEFAULT_PORT)){
            throw new IllegalArgumentException(
                    String.format("Invalid host %s or port %s", host, port));
        }

        this.host = Constants.DEFAULT_HOST;
        this.port = Constants.DEFAULT_PORT;
        this.HASH_PREFIX = Constants.DEFAULT_HASH_PREFIX;
        this.QUEUE_KEY   = Constants.DEFAULT_QUEUE_PREFIX;
        jedis = new Jedis(this.host, this.port);

        loadLUA();
    }

    public JedisJobStore(String hashPrefix, String queuePrefix) throws IllegalArgumentException{

        if(hashPrefix == null || hashPrefix.isEmpty()
                || queuePrefix == null || queuePrefix.isEmpty()){
            throw new IllegalArgumentException(
                    String.format("Invalid prefix supplied, hash: %s, queue: %s",hashPrefix, queuePrefix));
        }

        if(!testJedisConnection(Constants.DEFAULT_HOST, Constants.DEFAULT_PORT)){
            throw new IllegalArgumentException(
                    String.format("Invalid host %s or port %s", host, port));
        }

        this.host = Constants.DEFAULT_HOST;
        this.port = Constants.DEFAULT_PORT;
        this.HASH_PREFIX = hashPrefix;
        this.QUEUE_KEY   = queuePrefix;
        jedis = new Jedis(this.host, this.port);

        loadLUA();
    }

    public JedisJobStore(String host, int port, String hashPrefix, String queuePrefix) throws IllegalArgumentException{

        if(hashPrefix == null || hashPrefix.isEmpty()
                || queuePrefix == null || queuePrefix.isEmpty()){
            throw new IllegalArgumentException(
                    String.format("Invalid prefix supplied, hash: %s, queue: %s",hashPrefix, queuePrefix));
        }

        if(!testJedisConnection(host,port)){
            throw new IllegalArgumentException(String.format("Invalid host %s or port %s", host, port));
        }

        this.host = host;
        this.port = port;
        this.HASH_PREFIX = hashPrefix;
        this.QUEUE_KEY   = queuePrefix;
        jedis = new Jedis(host,port);

        loadLUA();
    }


    public void loadLUA(){
        if(scriptLoaded.compareAndSet(false, true)) {
            scriptSHA = jedis.scriptLoad(fetchJobScript);
        }
    }

    public void flushLUA(){
        jedis.scriptFlush();
    }

    private boolean addJob(JobDetail detail, String id, long timeOut){

        boolean success = false;
        try {
            Transaction t = jedis.multi();
            t.hmset(HASH_PREFIX + id, detail.transform());
            t.zadd(QUEUE_KEY, timeOut, id);
            t.exec();
            success = true;
        }catch (Exception e){
            logger.error(e);
        }
        return success;
    }

    private boolean updateJobState(String id, JobState state){
        boolean success = false;
        try {
            Transaction t = jedis.multi();
            t.hset(HASH_PREFIX + id, "state", state.toString());
            t.exec();
            success = true;
        }catch (Exception e){
            logger.error(e);
        }
        return success;
    }

    private JobDetail formJobDetail(JobRequest.Request request, long delay){

        String id = generateID();
        JobRequest.Request request1 = JobRequest.Request.newBuilder(request).setId(id).build();
        JobDetail detail = new JobDetail(id, request1.toString(), "IDLE ", System.currentTimeMillis(), delay * 1000);

        return detail;
    }

    private String fetchQueuedJobId() {

        String jobId = (String)jedis.evalsha(scriptSHA,
                Arrays.asList(QUEUE_KEY),
                Arrays.asList(String.valueOf(System.currentTimeMillis())));

        if(jobId == null || jobId.isEmpty()){
            return null;
        }

        return jobId;
    }

    private boolean verifyJobRequest(JobRequest.Request request){
        boolean success = false;
        do{
            if(request == null){
                break;
            }

            if(request.getContent() == null || request.getContent().isEmpty()){
                break;
            }

            success = true;
        }while (false);

        return success;
    }


    @Override
    public synchronized boolean submitJobRequest(JobRequest.Request request){

        boolean success = false;

        if(!verifyJobRequest(request)){
            return success;
        }

        JobDetail detail = formJobDetail(request, 0);
        success = addJob(detail, detail.getId(), detail.getCreatedOn());

        return success;
    }

    @Override
    public synchronized boolean submitDelayedJobRequest(JobRequest.Request request, int after){
        boolean success = false;

        if(!verifyJobRequest(request)){
            return success;
        }

        JobDetail detail = formJobDetail(request, after);
        success = addJob(detail, detail.getId(), detail.getCreatedOn() + (after * 1000));
        return success;
    }

    @Override
    public synchronized JobDetail fetchQueuedJob(){

        String jobId = fetchQueuedJobId();
        JobDetail detail = null;

        if(jobId != null && !jobId.isEmpty()){

            updateJobState(jobId, JobState.RUNNING);
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
    public synchronized boolean jobCompleted(String id, boolean success){
        JobState state = JobState.FAILED;
        if(success) {
            state = JobState.SUCCEEDED;
        }

        return updateJobState(id, state);
    }

    /*TODO: Prevent deletion of job if the state of the job is running, requires a LUA script */
    @Override
    public synchronized boolean deleteJob(String id){

        boolean success = false;
        try {
            Transaction t = jedis.multi();
            t.del(id);
            t.zrem(QUEUE_KEY, id);
            t.exec();
            success = true;
        }catch (Exception e){
            logger.error(e);
        }

        return success;
    }
}
