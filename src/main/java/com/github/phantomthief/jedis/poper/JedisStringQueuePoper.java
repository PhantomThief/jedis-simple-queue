/**
 * 
 */
package com.github.phantomthief.jedis.poper;

import java.util.function.Function;
import java.util.function.Supplier;

import redis.clients.jedis.ShardedJedisPool;

/**
 * @author w.vela
 */
public class JedisStringQueuePoper<E> extends AbsJedisQueuePoper<String, String, String> {

    private static final int DEFAULT_WAIT = 2;

    public JedisStringQueuePoper(String queueKey, Supplier<ShardedJedisPool> jedisFactory) {
        this(queueKey, jedisFactory, DEFAULT_WAIT);
    }

    /**
     * @param queueKey
     * @param jedisFactory
     * @param wait
     */
    public JedisStringQueuePoper(String queueKey, Supplier<ShardedJedisPool> jedisFactory, int wait) {
        super(queueKey, jedisFactory, (j, k) -> j.brpop(wait, k), Function.identity());
    }

}
