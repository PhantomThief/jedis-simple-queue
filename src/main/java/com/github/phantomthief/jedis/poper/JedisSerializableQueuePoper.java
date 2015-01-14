/**
 * 
 */
package com.github.phantomthief.jedis.poper;

import java.io.Serializable;
import java.util.function.Supplier;

import org.apache.commons.lang3.SerializationUtils;

import redis.clients.jedis.ShardedJedisPool;

/**
 * @author w.vela
 */
public class JedisSerializableQueuePoper<E extends Serializable> extends
        AbsJedisQueuePoper<byte[], E, byte[]> {

    private static final int DEFAULT_WAIT = 2;

    public JedisSerializableQueuePoper(byte[] queueKey, Supplier<ShardedJedisPool> jedisFactory) {
        this(queueKey, jedisFactory, DEFAULT_WAIT);
    }

    /**
     * @param queueKey
     * @param jedisFactory
     * @param wait
     */
    public JedisSerializableQueuePoper(byte[] queueKey, Supplier<ShardedJedisPool> jedisFactory,
            int wait) {
        super(queueKey, jedisFactory, (j, k) -> j.brpop(wait, k), SerializationUtils::deserialize);
    }

}
