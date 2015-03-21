/**
 * 
 */
package com.github.phantomthief.jedis.poper;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

import redis.clients.jedis.JedisPool;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author w.vela
 */
public class ListJedisJsonPoper<E> extends AbsListJedisQueuePoper<String, E, String> {

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
    public ListJedisJsonPoper(String queueKey, Supplier<List<JedisPool>> jedisFactory, Class<E> type) {
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
