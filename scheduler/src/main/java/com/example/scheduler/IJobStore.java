package com.example.scheduler;

public interface IJobStore {
    public void jobCompleted(String id, boolean success);
    public void submitJobRequest(JobRequest.Request request);
    public void submitDelayedJobRequest(JobRequest.Request request, int after);
    public JobDetail fetchTimedOutJob();
    public void deleteJob(String id);
    public JobDetail fetchJob(String id);
}
