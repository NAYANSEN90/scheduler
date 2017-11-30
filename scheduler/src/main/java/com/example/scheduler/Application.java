package com.example.scheduler;


import com.example.scheduler.verticles.JobSubmitterVerticle;
import com.example.scheduler.verticles.JobWorkerVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;

public class Application {

    private static Vertx vertx;

    public static void main(String args[]){

        vertx = Vertx.vertx();
        //JedisJobStore store = new JedisJobStore();

        //store.submitJobRequest(JobRequest.Request.newBuilder().setContent("Hello world Job").build());
        //store.loadLUA();
        //JobDetail detail = store.fetchQueuedJob();

        //System.out.println(detail);
        //store.flushLUA();

        vertx.deployVerticle(new JobWorkerVerticle("JobWorker1", new JedisJobStore()), new DeploymentOptions().setWorker(true));
        vertx.deployVerticle(new JobWorkerVerticle("JobWorker2", new JedisJobStore()), new DeploymentOptions().setWorker(true));

        vertx.deployVerticle(new JobSubmitterVerticle("JobSubmitter1", new JedisJobStore()), new DeploymentOptions().setWorker(true));



    }
}
