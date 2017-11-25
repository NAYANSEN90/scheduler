package com.example.scheduler;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class JobDetail {
    /* holds the string representation of Job */
    private String request;
    private String id;
    /* determines the state of the job */
    private String state;
    /* time params in ms */
    private long createdOn;
    private long initialDelay;


    public Map<String, String> transform(){
        Map<String, String> data = new HashMap<>();
        data.put("request", request);
        data.put("id", id);
        data.put("state", state);
        data.put("created", String.valueOf(createdOn));
        data.put("delay", String.valueOf(initialDelay));

        return data;
    }

    public JobDetail(){

    }

    public JobDetail(String id, String request, String state, long createdOn, long delay){
        this.id = id;
        this.request = request;
        this.state = state;
        this.createdOn = createdOn;
        this.initialDelay = delay;
    }

    public JobDetail(Map<String, String> data){
        this.id = data.get("id");
        this.request = data.get("request");
        this.state = data.get("state");
        this.createdOn = Long.parseLong(data.get("created"));
        this.initialDelay = Long.parseLong(data.get("delay"));
    }
}
