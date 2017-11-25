package com.nrxen.scheduler;

public class JobHarness {
    /* holds the string representation of Job */
    private String request;
    private String id;
    /* determines the state of the job */
    private String state;
    /* time params in ms */
    private long createdOn;
    private long initialDelay;
}
