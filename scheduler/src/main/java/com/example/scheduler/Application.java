package com.example.scheduler;


public class Application {

    public static void main(String args[]){

        JedisJobStore store = new JedisJobStore();
        store.submitJobRequest(JobRequest.Request.newBuilder().setContent("Hello world Job").build());
        store.loadLUAScript();
        JobDetail detail = store.fetchJob();
    }
}
