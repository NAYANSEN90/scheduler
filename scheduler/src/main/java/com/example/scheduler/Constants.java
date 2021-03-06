package com.example.scheduler;

public class Constants {
    public static final String DEFAULT_HOST = "127.0.0.1";
    public static final int    DEFAULT_PORT = 6379;
    public static final String DEFAULT_HASH_PREFIX = "JOB:";
    public static final String DEFAULT_QUEUE_PREFIX = "JOB_QUEUE";

    public static final String FETCH_JOB_LUA = "fetchJob.lua";
    public static final String DELETE_JOB_LUA = "deleteJob.lua";
    public static final String GENERATE_ID_LUA = "generateID.lua";


    /* Link to the job states that is defined */
    public static final int JOB_QUEUED_LUA     = 0;
    public static final int JOB_RUNNING_LUA    = 1;
    public static final int JOB_SUCCEEDED_LUA  = 2;
    public static final int JOB_FAILED_LUA     = 3;

    public static final int JOB_STATES = 4;



}
