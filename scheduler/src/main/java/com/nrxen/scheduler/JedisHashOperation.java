package com.nrxen.scheduler;

import redis.clients.jedis.Jedis;

public class JedisHashOperation implements IHashOperation {

    private String host = "127.0.0.1";
    private int port = 6379;

    private Jedis jedis = new Jedis(host, port) ;

    public JedisHashOperation(){

    }
}
