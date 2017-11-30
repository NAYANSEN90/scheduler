package com.example.scheduler.verticles;

import com.example.scheduler.IJobStore;
import com.example.scheduler.JobDetail;
import io.vertx.core.AbstractVerticle;

import java.util.Random;

public class JobWorkerVerticle extends AbstractVerticle {

    private String id;
    private IJobStore store;

    public static final String WORKER_ADDRESS = "WORKER_ADDRESS";
    private Thread workerThread;
    private boolean polling = true;


    public JobWorkerVerticle(String id, IJobStore store){
        this.id = id;
        this.store = store;
    }

    public void start(){


        workerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while(polling){
                    JobDetail detail = store.fetchQueuedJob();
                    if(detail != null) {
                        System.out.println("-----------------------------------");
                        System.out.println("Fetched job: " + System.currentTimeMillis());
                        System.out.println(id + "::::" + detail);
                        System.out.println("-----------------------------------");

                        try {
                            // sample work
                            long time = new Random().nextInt(100);
                            Thread.currentThread().sleep(time);

                        }catch (Exception e){

                        }

                        boolean deleted = store.deleteJob(detail.getId());
                        if(deleted){
                            System.out.println("Job " + detail.getId() + " is deleted");
                        }
                        store.jobCompleted(detail.getId(), true);


                        JobDetail detail1 = store.fetchJob( detail.getId() );
                        if(detail1 != null) {
                            System.out.println("-----------------------------------");
                            System.out.println("Completed job: " + System.currentTimeMillis());
                            System.out.println(id + "::::" + detail1);
                            System.out.println("-----------------------------------");
                        }
                    }


                    try {
                        Thread.currentThread().sleep(1000);

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
