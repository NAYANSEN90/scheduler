package com.nrxen.scheduler

import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory
import io.vertx.redis.RedisClient
import io.vertx.redis.RedisOptions

class JobStore(host: String, port: Int) {

    private val connection: RedisOptions?
    private val client: RedisClient?

    init {
        connection = RedisOptions().setAddress(host).setPort(port)
        client = RedisClient.create(Vertx.vertx(), connection!!)
        testRedisConnection()
    }


    private fun testRedisConnection(): Boolean {

        val success = false

        if (connection == null || client == null) {
            return success
        }

        client.ping { stringAsyncResult ->
            if (stringAsyncResult.succeeded()) {
                logger.info(stringAsyncResult.result())
            } else {
                logger.error("Failed to ping redis server")
            }
        }

        return success
    }

    companion object {
        private val JOB_QUEUE = "JOB_QUEUE"
        private val logger = LoggerFactory.getLogger(JobStore::class.java)
    }
}
