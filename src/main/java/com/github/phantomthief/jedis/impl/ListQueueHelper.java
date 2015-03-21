/**
 * 
 */
package com.github.phantomthief.jedis.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import com.github.phantomthief.jedis.util.CloseableList;

/**
 * @author w.vela
 */
public class ListQueueHelper {

    private final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(getClass());

    private final Supplier<List<JedisPool>> jedisFactory;

    public ListQueueHelper(Supplier<List<JedisPool>> jedisFactory) {
        this.jedisFactory = jedisFactory;
    }

    public void enqueue(String key, String data) {
        List<JedisPool> pool = jedisFactory.get();
        try (CloseableList<Jedis> allShards = pool.stream().map(JedisPool::getResource)
                .collect(Collectors.toCollection(CloseableList::new))) {
            Collections.shuffle(allShards);
            for (Jedis jedis : allShards) {
                try {
                    jedis.lpush(key, data);
                    return;
                } catch (Throwable e) {
                    logger.error("fail to enqueue [{}:{}], {}->{}, exception:{}", jedis.getClient()
                            .getHost(), jedis.getClient().getPort(), key, data, e.getMessage());
                }
            }
        } catch (IOException e) {
            logger.error("Ops.", e);
        }
        throw new RuntimeException("fail to enqueue:" + key + ", " + data);
    }

    public void enqueue(byte[] key, byte[] data) {
        List<JedisPool> pool = jedisFactory.get();
        try (CloseableList<Jedis> allShards = pool.stream().map(JedisPool::getResource)
                .collect(Collectors.toCollection(CloseableList::new))) {
            Collections.shuffle(allShards);
            for (Jedis jedis : allShards) {
                try {
                    jedis.lpush(key, data);
                    return;
                } catch (Throwable e) {
                    logger.error("fail to enqueue [{}:{}], {}->{}, exception:{}", jedis.getClient()
                            .getHost(), jedis.getClient().getPort(), new String(key), data, e
                            .getMessage());
                }
            }
        } catch (IOException e) {
            logger.error("Ops.", e);
        }
        throw new RuntimeException("fail to enqueue:" + new String(key) + ", "
                + Arrays.toString(data));
    }

}
