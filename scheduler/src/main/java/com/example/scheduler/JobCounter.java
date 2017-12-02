package com.example.scheduler;

import lombok.Data;

import java.util.concurrent.atomic.AtomicInteger;

@Data
public class JobCounter{
    private  String name;
    private AtomicInteger submitted = new AtomicInteger(0);
    private AtomicInteger success   = new AtomicInteger(0);
    private AtomicInteger failed    = new AtomicInteger(0);
    private AtomicInteger running   = new AtomicInteger(0);

    public void updateSubmitted(){
        submitted.incrementAndGet();
    }

    public void updateSuccess(){
        success.incrementAndGet();
    }

    public void updateFailed(){
        failed.incrementAndGet();
    }

    public void updateRunning(){
        running.incrementAndGet();
    }

    public int submitted(){
        return submitted.get();
    }

    public int success(){
        return success.get();
    }

    public int failed(){
        return failed.get();
    }

    public int running(){
        return running.get();
    }

    public JobCounter(String name){
        this.name = name;
    }

    public String getStat(){
        return String.format("Store:%s\n" +
                "SUBMITTED: %s\n" +
                "SUCCESS: %s\n" +
                "FAILED: %s\n" +
                "RUNNING: %s\n", name, submitted(), success(), failed(), running());

    }
}
