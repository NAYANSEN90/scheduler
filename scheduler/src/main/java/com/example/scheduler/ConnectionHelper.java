package com.example.scheduler;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class ConnectionHelper {

    private static Logger logger = LoggerFactory.getLogger(ConnectionHelper.class);

    public static boolean testJedisConnection(final String host, final int port){
        boolean success = false;
        Jedis tryJedis = null;
        try {

            do {

                if (host == null || host.isEmpty()) {
                    break;
                }
                if (port <= 0) {
                    break;
                }
                tryJedis = new Jedis(host, port);
                String pong = "";
                try {
                    pong = tryJedis.ping();
                } catch (Exception e) {
                    logger.error("Exception: ", e);
                    break;
                }
                if (!pong.equals("PONG")) {
                    break;
                }

                success = true;
            } while (false);
        }finally {
            if(tryJedis != null) {
                tryJedis.close();
            }
        }

        return success;
    }

    private static boolean testJedisConnection(final JedisPool pool){
        boolean success = false;
        if(pool == null || pool.isClosed()){
            return success;
        }

        try(Jedis jedis = pool.getResource()){
            String pong = jedis.ping();
            if(pong.equals("PONG")) {
                success = true;
            }
        }catch (Exception e){
            logger.error("Exception: ", e);
        }
        return success;
    }
}
