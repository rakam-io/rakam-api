package org.rakam.cache;

import java.util.EventListener;

/**
 * Created by buremba on 24/05/14.
 */
public interface MessageListener<E> extends EventListener {
    void onMessage(E message);
}
