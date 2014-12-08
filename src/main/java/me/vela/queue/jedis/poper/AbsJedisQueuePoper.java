/**
 * 
 */
package me.vela.queue.jedis.poper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import me.vela.queue.jedis.util.WeightTreeInfo;

import org.apache.commons.lang3.RandomUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

/**
 * @author w.vela
 */
public abstract class AbsJedisQueuePoper<K, E, R> implements Supplier<E> {

    private final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(getClass());

    private static final long DEFAULT_WAIT_ON_EMPTY = TimeUnit.SECONDS.toMillis(30);

    private final K queueKey;

    private final Supplier<ShardedJedisPool> jedisFactory;

    private final BiFunction<Jedis, K, List<R>> poper;

    private final Function<R, E> decoder;

    /**
     * @param queueKey
     * @param jedisFactory
     * @param poper
     * @param decoder
     */
    protected AbsJedisQueuePoper(K queueKey, Supplier<ShardedJedisPool> jedisFactory,
            BiFunction<Jedis, K, List<R>> poper, Function<R, E> decoder) {
        this.queueKey = queueKey;
        this.jedisFactory = jedisFactory;
        this.poper = poper;
        this.decoder = decoder;
    }

    /* (non-Javadoc)
     * @see java.util.function.Supplier#get()
     */
    @Override
    public E get() {
        ShardedJedisPool pool = jedisFactory.get();
        try (ShardedJedis resource = pool.getResource()) {
            List<Jedis> allShards = new ArrayList<>(resource.getAllShards());
            WeightTreeInfo<Jedis> sorted = new WeightTreeInfo<>();
            for (Jedis j : allShards) {
                long length = 0;
                try {
                    if (queueKey instanceof byte[]) {
                        length = j.llen((byte[]) queueKey);
                    } else if (queueKey instanceof String) {
                        length = j.llen((String) queueKey);
                    }
                } catch (Throwable e) {
                    logger.warn("queue length fail:{},{}", queueKey, e.getMessage());
                }
                if (length > 0) {
                    sorted.putNode(j, length);
                }
            }
            if (allShards.isEmpty()) {
                try {
                    Thread.sleep(DEFAULT_WAIT_ON_EMPTY);
                } catch (InterruptedException e) {
                    logger.error("Ops.", e);
                }
                return null;
            }
            Jedis j;
            if (sorted.isEmpty()) {
                // 如果为空就随机选一个
                j = allShards.get(RandomUtils.nextInt(0, allShards.size()));
            } else {
                j = sorted.getNode();
            }
            List<R> brpop = poper.apply(j, queueKey);
            if (brpop == null) {
                return null;
            }
            for (R bs : brpop) {
                if (bs instanceof byte[]) {
                    if (Arrays.equals((byte[]) bs, (byte[]) queueKey)) {
                        continue;
                    }
                } else {
                    if (Objects.equals(bs, queueKey)) {
                        continue;
                    }
                }
                return decoder.apply(bs);
            }
            return null;
        }
    }

    @Override
    public String toString() {
        return "AbsJedisQueuePoper [queueKey=" + queueKey + ", poper=" + poper + ", decoder="
                + decoder + "]";
    }

}
