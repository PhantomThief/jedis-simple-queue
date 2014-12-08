/**
 * 
 */
package me.vela.queue.jedis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

/**
 * @author w.vela
 */
public final class QueueHelper {

    private final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(getClass());

    private final Supplier<ShardedJedisPool> jedisFactory;

    public QueueHelper(Supplier<ShardedJedisPool> jedisFactory) {
        this.jedisFactory = jedisFactory;
    }

    public void enqueue(String key, String data) {
        ShardedJedisPool pool = jedisFactory.get();
        try (ShardedJedis resource = pool.getResource()) {
            List<Jedis> allShards = new ArrayList<>(resource.getAllShards());
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
        }
        throw new RuntimeException("fail to enqueue:" + key + ", " + data);
    }

    public void enqueue(byte[] key, byte[] data) {
        ShardedJedisPool pool = jedisFactory.get();
        try (ShardedJedis resource = pool.getResource()) {
            List<Jedis> allShards = new ArrayList<>(resource.getAllShards());
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
        }
        throw new RuntimeException("fail to enqueue:" + new String(key) + ", "
                + Arrays.toString(data));
    }

}
