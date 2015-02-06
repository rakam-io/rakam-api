package org.rakam.util;

/**
 * Created by buremba <Burak Emre Kabakcı> on 25/10/14 19:56.
 */
@FunctionalInterface
public interface Handler<T> {
    public void handle(T obj);
}
