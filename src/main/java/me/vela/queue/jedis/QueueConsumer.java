/**
 * 
 */
package me.vela.queue.jedis;

import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author w.vela
 */
public class QueueConsumer<T> {

    private final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(getClass());

    private final Supplier<T> queuePoper;

    private final Consumer<T> queueConsumer;

    private volatile boolean stopped = false;

    /**
     * @param queuePoper
     * @param queueConsumer
     */
    public QueueConsumer(Supplier<T> queuePoper, Consumer<T> queueConsumer) {
        this.queuePoper = queuePoper;
        this.queueConsumer = queueConsumer;
    }

    public void startConsume() {
        while (!stopped) {
            try {
                T object = queuePoper.get();
                if (object != null) {
                    queueConsumer.accept(object);
                }
            } catch (Throwable e) {
                logger.error("fail to consumer:{}, {}, exception:{}", queuePoper, queueConsumer,
                        e.getMessage());
            }
        }
    }

    public void stop() {
        stopped = true;
    }

}
