/**
 * 
 */
package com.github.phantomthief.jedis.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author w.vela
 */
public class CloseableList<E extends Closeable> extends ArrayList<E> implements Closeable {

    private static final long serialVersionUID = 3014352218778887709L;

    /* (non-Javadoc)
     * @see java.io.Closeable#close()
     */
    @Override
    public void close() throws IOException {
        List<IOException> exps = new ArrayList<>();
        for (E node : this) {
            try {
                node.close();
            } catch (IOException e) {
                exps.add(e);
            }
        }
        if (!exps.isEmpty()) {
            throw exps.get(0);
        }
    }

}
