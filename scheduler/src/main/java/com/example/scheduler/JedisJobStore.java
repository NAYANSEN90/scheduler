package com.example.scheduler;


import java.util.UUID;

public class JedisJobStore implements IJobStore {

    private JedisQueueOperation queueOperation = new JedisQueueOperation();
    private JedisHashOperation  hashOperation  = new JedisHashOperation();

    public JedisJobStore(){
        queueOperation.setQueue("JOB_QUEUE");
        hashOperation.setHashPrefix("JOB:");
    }

    private String generateID(){
        return UUID.randomUUID().toString();
    }

    public synchronized void submitJobRequest(JobRequest.Request request){
        /* generate id */
        /* push to hashmap*/
        /* push to queue */
    }

    public synchronized void submitDelayedJobRequest(JobRequest.Request request, int after){

    }
}
