package com.nrxen.scheduler;

import java.util.HashMap;
import java.util.Map;

public class JobHarness {
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
}
