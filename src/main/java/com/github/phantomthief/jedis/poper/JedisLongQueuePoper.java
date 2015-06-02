/**
 * 
 */
package com.github.phantomthief.jedis.poper;

import java.util.function.Supplier;

import org.apache.commons.lang3.math.NumberUtils;

import redis.clients.jedis.ShardedJedisPool;

/**
 * @author w.vela
 */
public class JedisLongQueuePoper extends AbsJedisQueuePoper<String, Long, String> {

    private static final int DEFAULT_WAIT = 2;

    public JedisLongQueuePoper(String queueKey, Supplier<ShardedJedisPool> jedisFactory) {
        this(queueKey, jedisFactory, DEFAULT_WAIT);
    }

    /**
     * @param queueKey
     * @param jedisFactory
     * @param wait
     */
    public JedisLongQueuePoper(String queueKey, Supplier<ShardedJedisPool> jedisFactory, int wait) {
        super(queueKey, jedisFactory, (j, k) -> j.brpop(wait, k), NumberUtils::toLong);
    }

}
