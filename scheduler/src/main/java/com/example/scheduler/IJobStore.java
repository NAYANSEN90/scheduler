package com.example.scheduler;

import java.util.UUID;

public interface IJobStore {
    public boolean     jobCompleted(String id, boolean success);
    public String      submitJobRequest(String request);
    public String      submitDelayedJobRequest(String request, int after);
    public JobDetail   popQueuedJob();
    public boolean     deleteJob(String id);
    public JobDetail   peekJob(String id);
    public String      getStat();

    default public String generateID(){
        return UUID.randomUUID().toString();
    }
}
