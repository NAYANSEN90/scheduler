package com.example.scheduler.verticles;

import com.example.scheduler.IJobStore;
import com.example.scheduler.JobRequest;
import io.vertx.core.AbstractVerticle;

import java.util.concurrent.atomic.AtomicBoolean;

public class JobSubmitterVerticle  extends AbstractVerticle{

    private String id;
    private IJobStore store;
    private AtomicBoolean running = new AtomicBoolean(false);
    private boolean abort = false;
    private int count = 10;
    private Thread submitter;

    public JobSubmitterVerticle(String id, IJobStore store){
        this.id = id;
        this.store = store;
    }

    public void start(){
        if(running.compareAndSet(false,true)){
            submitter = new Thread(new Runnable() {
                @Override
                public void run() {
                    while(count > 0){
                        try{
                            store.submitDelayedJobRequest(
                                    JobRequest.Request.newBuilder().setContent("Hello world" + count).build(), 0);
                            Thread.currentThread().sleep(1000);
                            vertx.eventBus().send(JobWorkerVerticle.WORKER_ADDRESS, "JobSubmitted");
                            count --;

                        }catch (Exception e){

                        }
                    }
                }
            });

            submitter.start();
        }

    }
}
