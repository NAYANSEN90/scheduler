package com.example.scheduler;

public interface IJobStore {
    public boolean jobCompleted(String id, boolean success);
    public boolean submitJobRequest(JobRequest.Request request);
    public boolean submitDelayedJobRequest(JobRequest.Request request, int after);
    public JobDetail fetchQueuedJob();
    public boolean deleteJob(String id);
    public JobDetail fetchJob(String id);
}
