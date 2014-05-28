package org.rakam.cache;

import org.rakam.database.KeyValueStorage;
import org.vertx.java.core.json.JsonObject;

/**
 * Created by buremba on 21/12/13.
 */
public interface CacheAdapter extends KeyValueStorage {
    public abstract void addGroupByItem(String aggregation, String groupBy, String item);
    public abstract void addGroupByItem(String aggregation, String groupBy, String item, Long incrementBy);
    public abstract JsonObject getActorProperties(String project, String actor_id);
    public abstract void addActorProperties(String project, String actor_id, JsonObject properties);
    public abstract void setActorProperties(String project, String actor_id, JsonObject properties);
    public abstract void flush();
}
