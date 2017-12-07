package com.example.scheduler;


import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.example.scheduler.JobState.FAILED;
import static com.example.scheduler.JobState.SUCCEEDED;
import static com.example.scheduler.JobState.QUEUED;


public class JedisJobStore implements IJobStore{

    private static Logger logger = LoggerFactory.getLogger(JedisJobStore.class);

    private String host;
    private int    port;

    private JedisPool pool;

    private String HASH;
    private String QUEUED_SET;
    private String RUNNING_SET;
    private String SUCCESS_SET;
    private String FAILED_SET;
    private String JOB_COUNTER;

    private AtomicBoolean scriptLoaded = new AtomicBoolean(false);


    private String fetchJobLUA;
    private String deleteJobLUA;
    private String generateIDLUA;

    private String fetchJobSHA;
    private String deleteJobSHA;
    private String generateIDSHA;
    private String jobStoreName;

    private void assignStoreName(String name) throws IllegalArgumentException{
        if(name == null || name.isEmpty()){
            throw new IllegalArgumentException("Invalid store name");
        }

        jobStoreName = name;
        logger.info("Creating store with name: " + jobStoreName);
    }

    private void initDataStructures(String name){
        this.HASH         = String.format("%s:JOB:", name);
        this.QUEUED_SET   = String.format("%s:QUEUED", name);
        this.RUNNING_SET  = String.format("%s:RUNNING", name);
        this.SUCCESS_SET  = String.format("%s:SUCCESS", name);
        this.FAILED_SET   = String.format("%s:FAILED", name);
        this.JOB_COUNTER  = String.format("%s:COUNTER", name);
    }

    private void initStore(String host, int port, String name){
        this.host = host;
        this.port = port;
        this.pool = new JedisPool(this.host, this.port);
        assignStoreName(name);
        initDataStructures(name);
    }

    private void initStore(String name, JedisPool pool){
        this.host = "0.0.0.0";
        this.port = 0;
        this.pool = pool;
        assignStoreName(name);
        initDataStructures(name);
    }

    public void closeStore(){
        if(this.pool != null){
            this.pool.close();
        }
    }

    private Jedis getResource(){
        return this.pool.getResource();
    }

    /** CONSTRUCTORS **/
    public JedisJobStore(String name) throws IllegalArgumentException{

        if(!ConnectionHelper.testJedisConnection(Constants.DEFAULT_HOST, Constants.DEFAULT_PORT)){
            throw new IllegalArgumentException(
                    String.format("Invalid host %s or port %s", Constants.DEFAULT_HOST, Constants.DEFAULT_PORT));
        }

        initStore(Constants.DEFAULT_HOST, Constants.DEFAULT_PORT,
                name);

        loadLUA();
    }

    public JedisJobStore(String name, String host, int port)
            throws IllegalArgumentException{

        if(!ConnectionHelper.testJedisConnection(host,port)){
            throw new IllegalArgumentException(String.format("Invalid host %s or port %s", host, port));
        }

        initStore(host, port, name);
        loadLUA();
    }

    public JedisJobStore(String name, JedisPool pool)
            throws IllegalArgumentException{

        if(!ConnectionHelper.testJedisConnection(pool)){
            throw  new IllegalArgumentException("Invalid jedis pool supplied");
        }
        initStore(name, pool);
        loadLUA();
    }

    public boolean loadLUA(){

        try(Jedis jedis = getResource()){

            fetchJobLUA   = ResourceUtil.fetchLUA(Constants.FETCH_JOB_LUA);
            deleteJobLUA  = ResourceUtil.fetchLUA(Constants.DELETE_JOB_LUA);
            generateIDLUA = ResourceUtil.fetchLUA(Constants.GENERATE_ID_LUA);

            fetchJobSHA   = jedis.scriptLoad(fetchJobLUA);
            deleteJobSHA  = jedis.scriptLoad(deleteJobLUA);
            generateIDSHA = jedis.scriptLoad(generateIDLUA);

            scriptLoaded.compareAndSet(false,true);

        }catch (Exception e){
            logger.error("Exception:", e);
        }

        return scriptLoaded.get();

    }

    /* shouldn't be called, as it might invalidate other scripts running in the system */
    public void flushLUA(){
        try(Jedis jedis = getResource()) {
            jedis.scriptFlush();
        }
    }


    private boolean addJob(JobDetail detail, String id, long timeOut){

        boolean success = false;
        try(Jedis jedis = getResource()) {
            Transaction t = jedis.multi();
            t.hmset(HASH + id, detail.transform());
            t.zadd(QUEUED_SET, timeOut, id);
            t.exec();
            success = true;
        }catch (Exception e){
            logger.error("Exception:", e);
        }
        return success;
    }

    private boolean updateJobState(String id, JobState state){
        boolean success = false;
        String queue = "";

        if(state == SUCCEEDED){
            queue = SUCCESS_SET;
        } else {
            queue = FAILED_SET;
        }

        try(Jedis jedis = getResource()) {
            Transaction t = jedis.multi();
            t.hset(HASH+ id, "state", state.toString());
            t.zrem(RUNNING_SET, id); /* Remove the job from the running set */
            t.zadd(queue, System.currentTimeMillis(), id); /* Add the job to the corresponding completion set */
            t.exec();
            success = true;
        }catch (Exception ex){
            logger.error("Exception:" ,ex);
        }
        return success;
    }

    @Override
    public String generateID(){
        String ID = null;

        try(Jedis jedis = getResource()){
            Transaction t = jedis.multi();
            Response<String> id = t.evalsha(generateIDSHA, Arrays.asList(JOB_COUNTER),Arrays.asList());
            t.exec();

            if(id == null || id.get() == null || id.get().isEmpty()){
                return null;
            } else {
                ID = id.get();
            }
        }catch (Exception e){
            logger.error("Exception: ", e);
        }

        return ID;
    }

    private JobDetail formJobDetail(String request, long delay){

        String id = generateID();
        if(id == null){
            logger.error("Failed to generate Job Id");
            return null;
        }
        JobDetail detail = new JobDetail(id, request, QUEUED.toString(),
                System.currentTimeMillis(), delay * 1000);

        return detail;
    }

    private String fetchQueuedJobId(){

        String jobId = null;
        try(Jedis jedis = getResource()) {

            jobId = (String) jedis.evalsha(fetchJobSHA,
                    Arrays.asList(QUEUED_SET, RUNNING_SET, HASH),
                    Arrays.asList(String.valueOf(System.currentTimeMillis())));

            if (jobId == null || jobId.isEmpty()) {
                return null;
            }
        }catch (Exception e){
            logger.error("Exception:" , e);
        }

        return jobId;
    }

    private boolean verifyJobRequest(String request){
        boolean success = false;
        do{
            if(request == null || request.isEmpty()){
                break;
            }

            success = true;
        }while (false);

        return success;
    }

    /**
     * This function returns the current statistics of this job store
     * @return String a formatted statistics of this job store
     */
    @Override
    public String getStat(){

        long queued   = 0;
        long running  = 0;
        long success  = 0;
        long failed   = 0;

        try(Jedis jedis = getResource()){
            Transaction t = jedis.multi();
            Response<Long> queued1    = t.zcard(QUEUED_SET);
            Response<Long> running1   = t.zcard(RUNNING_SET);
            Response<Long> success1   = t.zcard(SUCCESS_SET);
            Response<Long> failed1    = t.zcard(FAILED_SET);
            t.exec();

            queued  = (queued1 != null)  ? queued1.get()  : 0;
            running = (running1 != null) ? running1.get() : 0;
            success = (success1 != null) ? success1.get() : 0;
            failed  = (failed1 != null)  ? failed1.get()  : 0;
        } catch (Exception e){
            logger.error("Exception: " , e);
        }

        return String.format("Store:%s\n" +
                "QUEUED  : %s\n" +
                "SUCCESS : %s\n" +
                "FAILED  : %s\n" +
                "RUNNING : %s\n", jobStoreName , queued, success, failed, running);

    }

    /**
     * The job is added immediately
     * @param request The job request containing the details about the job
     * @return boolean true/false if the job was submitted successfully or not
     */
    public String submitJobRequest(String request){

        String jobId = null;
        boolean success = false;

        if(!verifyJobRequest(request)){
            return null;
        }

        JobDetail detail = formJobDetail(request, 0);
        if(detail == null){
            return null;
        }
        success = addJob(detail, detail.getId(), detail.getCreatedOn());

        if(success){
            jobId = detail.getId();
        }

        return jobId;
    }

    /**
     * The job is added with a delay
     * @param request The job request containing details about the job
     * @param after The time delay in seconds
     * @return boolean true/false if the job was successfully submitted or not
     */
    public String submitDelayedJobRequest(String request, int after){
        boolean success = false;
        String jobId = null;

        if(!verifyJobRequest(request)){
            return null;
        }

        JobDetail detail = formJobDetail(request, after);
        if(detail == null){
            return null;
        }
        success = addJob(detail, detail.getId(), detail.getCreatedOn() + (after * 1000));
        if(success){
            jobId = detail.getId();
        }

        return jobId;
    }

    /**
     * Fetches a job that has timed out.
     * @return JobDetail: Details about the job where  along with the job request, the meta-data is also
     * available
     */
    @Override
    public JobDetail popQueuedJob(){

        String jobId = fetchQueuedJobId();
        JobDetail detail = null;

        if(jobId != null && !jobId.isEmpty()){
            detail = peekJob(jobId);
        }

        return detail;
    }

    /**
     * Peek a job that has been submitted without removing it from the job queue
     * @param id The id of the job to be peeked
     * @return JobDetail: Details about the job to be peeked containing the original request and other meta-data
     */
    @Override
    public JobDetail peekJob(String id){

        JobDetail detail = null;
        try(Jedis jedis = getResource()) {
            Transaction t1 = jedis.multi();
            Response<Map<String, String>> resp = t1.hgetAll(HASH + id);
            t1.exec();

            if (resp.get() != null && !resp.get().isEmpty()) {
                detail = new JobDetail(resp.get());
            }
        }catch (Exception e ){
            logger.error("Exception:", e);
        }

        return detail;
    }

    /**
     * Notify that a job has been completed successfully or failed. It updates the state of the job in the map.
     * @param id Id of the job to be updated
     * @param success Whether the job state was successfully completed or not
     * @return boolean true/false whether the state was updated or not
     */
    @Override
    public boolean jobCompleted(String id, boolean success){
        JobState state = FAILED;
        if(success) {
            state = SUCCEEDED;
        }

        return updateJobState(id, state);
    }

    /**
     * Delete a job with an id. If the job is currently in the running state, the job will not be deleted.
     * @param id The id of the job
     * @return true/false whether the job was successfully deleted or not
     */
    @Override
    public boolean deleteJob(String id){

        boolean success = false;
        try(Jedis jedis = getResource()) {
            Transaction t = jedis.multi();
            Response<String> resp = t.evalsha(deleteJobSHA,
                    Arrays.asList(QUEUED_SET, SUCCESS_SET, FAILED_SET, HASH+ id),
                    Arrays.asList(id));
            t.exec();

            int result = Integer.parseInt(resp.get());
            if(result == 1){
                success = true;
            }
        }catch (Exception e){
            logger.error("Exception:", e);
        }

        return success;
    }
}
