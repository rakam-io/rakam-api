package org.rakam.stream.kume;

import org.rakam.kume.Cluster;
import org.rakam.stream.ActorCacheAdapter;
import org.rakam.util.json.JsonObject;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * Created by buremba on 21/12/13.
 */

public class KumeStreamAdapter implements ActorCacheAdapter {
    private final Cluster cluster;

    public KumeStreamAdapter(Cluster cluster) throws IOException {
        this.cluster = cluster;
    }

    @Override
    public CompletableFuture<JsonObject> getActorProperties(String project, String actor_id) {
        return null;
    }

    @Override
    public void addActorProperties(String project, String actor_id, JsonObject properties) {

    }

    @Override
    public void setActorProperties(String project, String actor_id, JsonObject properties) {

    }
}
