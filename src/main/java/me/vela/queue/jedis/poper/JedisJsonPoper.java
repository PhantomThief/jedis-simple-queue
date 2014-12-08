/**
 * 
 */
package me.vela.queue.jedis.poper;

import java.io.IOException;
import java.util.function.Supplier;

import org.w3c.dom.events.Event;

import redis.clients.jedis.ShardedJedisPool;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author w.vela
 */
public class JedisJsonPoper<E extends Event> extends AbsJedisQueuePoper<String, E, String> {

    private static final int WAIT = 2;

    private static final ObjectMapper mapper = new ObjectMapper();
    static {
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    /**
     * @param queueKey
     * @param jedisFactory
     * @param poper
     * @param decoder
     */
    public JedisJsonPoper(String queueKey, Supplier<ShardedJedisPool> jedisFactory, Class<E> type) {
        super(queueKey, jedisFactory, (j, k) -> j.brpop(WAIT, k), raw -> readValue(raw, type));
    }

    private static <T> T readValue(String content, Class<T> valueType) {
        try {
            return mapper.readValue(content, valueType);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
