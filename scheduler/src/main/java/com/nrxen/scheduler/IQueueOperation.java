package com.nrxen.scheduler;

public interface IQueueOperation {

    public void setQueue(String queue);

    public void enqueueNow(JobRequest.Request request);

    //currently seconds assumed
    public void enqueueDelayed(JobRequest.Request request, int after);
}
