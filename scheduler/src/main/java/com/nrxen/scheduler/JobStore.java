package com.nrxen.scheduler;


import java.util.UUID;

public class JobStore {

    private JedisQueueOperation queueOperation = new JedisQueueOperation();

    public JobStore(){
        queueOperation.setQueue("JOB_QUEUE");
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
