package com.example.scheduler.verticles;

import com.example.scheduler.Application;
import com.example.scheduler.IJobStore;
import com.example.scheduler.JobDetail;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.Random;

public class JobWorkerVerticle extends AbstractVerticle {

    private String id;
    private IJobStore store;
    private Logger logger ;

    public static final String WORKER_ADDRESS = "WORKER_ADDRESS";
    private Thread workerThread;
    private boolean polling = true;


    public JobWorkerVerticle(String id, IJobStore store){
        this.id = id;
        this.store = store;
        this.logger  = LoggerFactory.getLogger(this.id);
    }

    public void start(){


        workerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while(polling){
                    JobDetail detail = store.popQueuedJob();
                    if(detail != null) {
                        /*
                        System.out.println("-----------------------------------");
                        System.out.println("Fetched job: " + System.currentTimeMillis());
                        System.out.println(id + "::::" + detail);
                        System.out.println("-----------------------------------");
                        */


                        logger.info("Processing job: " + detail.getId());


                        try {
                            // sample work
                            long time = new Random().nextInt(1000);
                            Thread.currentThread().sleep(time);

                        }catch (Exception e){

                        }

                        long counter = Application.jobProcessedCounter.incrementAndGet();
                        store.jobCompleted(detail.getId(), true);

                        /*
                        JobDetail detail1 = store.peekJob( detail.getId() );
                        if(detail1 != null) {
                            System.out.println("-----------------------------------");
                            System.out.println("Completed job: " + System.currentTimeMillis());
                            System.out.println(id + "::::" + detail1);
                            System.out.println("-----------------------------------");
                        }

                        try {
                            Thread.currentThread().sleep(100000);

                        }catch (Exception e){

                        }
                        */

                        if(counter % 100 == 0){
                            logger.info("--- Completed 100 jobs ---. Exiting" + id);
                            break;
                        }
                    }


                    try {
                        Thread.currentThread().sleep(10);

                    }catch (Exception e){

                    }
                }
            }
        });

        workerThread.start();

    }

    public void stop(){
        workerThread.interrupt();
        polling = false;
    }


}
