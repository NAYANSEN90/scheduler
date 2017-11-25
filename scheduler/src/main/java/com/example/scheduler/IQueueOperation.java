package com.example.scheduler;

public interface IQueueOperation {

    public void setQueue(String queue);

    public void enqueueNow(String id);

    //currently seconds assumed
    public void enqueueDelayed(String id, int after);

    public String dequeueOne();
}
