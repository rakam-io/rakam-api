package org.rakam.stream.kume;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import org.rakam.kume.Cluster;
import org.rakam.stream.ActorCacheAdapter;
import org.rakam.util.json.JsonObject;

import java.util.concurrent.CompletableFuture;

/**
 * Created by buremba on 21/12/13.
 */

public class KumeCacheAdapter implements ActorCacheAdapter {
    private Cluster cluster;

    @Inject
    public KumeCacheAdapter(Cluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public CompletableFuture<JsonObject> getActorProperties(String project, String actor_id) {
        return null;
    }

    @Override
    public void addActorProperties(String project, String actor_id, JsonNode properties) {

    }

    @Override
    public void setActorProperties(String project, String actor_id, JsonNode properties) {

    }
}
