package com.example.scheduler.verticles;

import com.example.scheduler.Application;
import com.example.scheduler.IJobStore;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;


import java.util.concurrent.atomic.AtomicBoolean;


public class JobSubmitterVerticle  extends AbstractVerticle{

    private String id;
    private IJobStore store;
    private AtomicBoolean running = new AtomicBoolean(false);
    private boolean abort = false;
    private int count = 100;
    private Thread submitter;
    private Logger logger;
    public JobSubmitterVerticle(String id, IJobStore store){
        this.id = id;
        this.store = store;
        this.logger = LoggerFactory.getLogger(this.id);
    }

    public void start(){
        if(running.compareAndSet(false,true)){
            submitter = new Thread(new Runnable() {
                @Override
                public void run() {
                    while(count > 0){
                        try{
                            store.submitDelayedJobRequest(
                                    "Hello world", 0);
                            Thread.currentThread().sleep(10);
                            //vertx.eventBus().send(JobWorkerVerticle.WORKER_ADDRESS, "JobSubmitted");
                            long counter = Application.jobSubmittedCounter.incrementAndGet();
                            logger.info("Submitting job number" + counter);
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
