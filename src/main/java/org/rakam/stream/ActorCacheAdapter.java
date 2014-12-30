package org.rakam.stream;

import org.rakam.util.json.JsonObject;

import java.util.concurrent.CompletableFuture;

/**
 * Created by buremba <Burak Emre Kabakcı> on 21/07/14 04:55.
 */
public interface ActorCacheAdapter {
    CompletableFuture<JsonObject> getActorProperties(String project, String actor_id);

    void addActorProperties(String project, String actor_id, JsonObject properties);

    void setActorProperties(String project, String actor_id, JsonObject properties);
}
