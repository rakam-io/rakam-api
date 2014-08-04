package org.rakam.cache;

import org.vertx.java.core.json.JsonObject;

/**
 * Created by buremba <Burak Emre Kabakcı> on 21/07/14 04:55.
 */
public interface ActorCacheAdapter {
    JsonObject getActorProperties(String project, String actor_id);
    void addActorProperties(String project, String actor_id, JsonObject properties);
    void setActorProperties(String project, String actor_id, JsonObject properties);
}
