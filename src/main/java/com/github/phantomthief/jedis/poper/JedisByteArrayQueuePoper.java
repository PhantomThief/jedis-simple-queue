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
public class JedisByteArrayQueuePoper extends AbsJedisQueuePoper<byte[], byte[], byte[]> {

    private static final int DEFAULT_WAIT = 2;

    public JedisByteArrayQueuePoper(byte[] queueKey, Supplier<ShardedJedisPool> jedisFactory) {
        this(queueKey, jedisFactory, DEFAULT_WAIT);
    }

    /**
     * @param queueKey
     * @param jedisFactory
     * @param wait
     */
    public JedisByteArrayQueuePoper(byte[] queueKey, Supplier<ShardedJedisPool> jedisFactory,
            int wait) {
        super(queueKey, jedisFactory, (j, k) -> j.brpop(wait, k), Function.identity());
    }

}
