/**
 * 
 */
package com.github.phantomthief.jedis;

import java.util.function.Supplier;

import redis.clients.jedis.ShardedJedisPool;

import com.github.phantomthief.jedis.impl.ShardedQueueHelper;

/**
 * @author w.vela
 */
public final class QueueHelper {

    private ShardedQueueHelper shardedHelper;

    @Deprecated
    public QueueHelper(Supplier<ShardedJedisPool> jedisFactory) {
        shardedHelper = new ShardedQueueHelper(jedisFactory);
    }

    public void enqueue(String key, String data) {
        shardedHelper.enqueue(key, data);
    }

    public void enqueue(byte[] key, byte[] data) {
        shardedHelper.enqueue(key, data);
    }

}
