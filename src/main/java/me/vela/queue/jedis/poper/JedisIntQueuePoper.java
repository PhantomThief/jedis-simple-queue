/**
 * 
 */
package me.vela.queue.jedis.poper;

import java.util.function.Supplier;

import org.apache.commons.lang3.math.NumberUtils;

import redis.clients.jedis.ShardedJedisPool;

/**
 * @author w.vela
 */
public class JedisIntQueuePoper<E> extends AbsJedisQueuePoper<String, Integer, String> {

    private static final int DEFAULT_WAIT = 2;

    public JedisIntQueuePoper(String queueKey, Supplier<ShardedJedisPool> jedisFactory) {
        this(queueKey, jedisFactory, DEFAULT_WAIT);
    }

    /**
     * @param queueKey
     * @param jedisFactory
     * @param wait
     */
    public JedisIntQueuePoper(String queueKey, Supplier<ShardedJedisPool> jedisFactory, int wait) {
        super(queueKey, jedisFactory, (j, k) -> j.brpop(wait, k), NumberUtils::toInt);
    }

}
