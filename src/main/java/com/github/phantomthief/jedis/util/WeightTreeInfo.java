/**
 * 
 */
package com.github.phantomthief.jedis.util;

import org.apache.commons.lang3.RandomUtils;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

/**
 * 带权重的树
 * 
 * @author w.vela
 * @param <T>
 */
public class WeightTreeInfo<T> {

    private static final int DEFAULT_WEIGHT = 100;

    private final RangeMap<Long, T> nodes = TreeRangeMap.create();

    private long maxWeight = 0;

    public void putNode(T node, long weight) {
        nodes.put(Range.closedOpen(maxWeight, maxWeight + weight), node);
        maxWeight += weight;
    }

    public T getNode() {
        if (isEmpty()) {
            return null;
        }
        long resultIndex = RandomUtils.nextLong(0, maxWeight);
        return nodes.get(resultIndex);
    }

    public static <T> WeightTreeInfo<T> buildSingleOne(T one) {
        WeightTreeInfo<T> result = new WeightTreeInfo<>();
        result.putNode(one, DEFAULT_WEIGHT);
        return result;
    }

    @Override
    public String toString() {
        return nodes.toString();
    }

    public boolean isEmpty() {
        return maxWeight == 0;
    }

}
