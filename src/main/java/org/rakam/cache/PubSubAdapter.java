package org.rakam.cache;

/**
 * Created by buremba on 24/05/14.
 */
public interface PubSubAdapter {
    public String subscribe(String id, MessageListener run);
    public void publish(String id, String message);
    public void desubscribe(String id, String subscription_id);
}