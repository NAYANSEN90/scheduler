package com.nrxen.scheduler;


public class Application {

    public static void main(String args[]){

        JobStore store = new JobStore("127.0.0.1",6379);
    }
}
