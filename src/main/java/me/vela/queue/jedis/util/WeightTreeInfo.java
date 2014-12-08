/**
 * 
 */
package me.vela.queue.jedis.util;

import org.apache.commons.lang3.RandomUtils;

import com.google.common.collect.Multiset;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeMultiset;
import com.google.common.collect.TreeRangeMap;

/**
 * 带权重的树
 * 
 * @author w.vela <wangtianzhou@diandian.com>
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

    public static void main(String[] args) {
        WeightTreeInfo<String> aInfo = new WeightTreeInfo<String>();
        aInfo.putNode("test1", 10);
        aInfo.putNode("test2", 40);
        aInfo.putNode("test3", 50);
        long t = System.currentTimeMillis();
        Multiset<String> result = TreeMultiset.create();
        for (int i = 0; i < 100000; i++) {
            result.add(aInfo.getNode());
        }
        System.out.println(result + ", cost:" + (System.currentTimeMillis() - t));
    }

    @Override
    public String toString() {
        return nodes.toString();
    }

    public boolean isEmpty() {
        return maxWeight == 0;
    }

}
