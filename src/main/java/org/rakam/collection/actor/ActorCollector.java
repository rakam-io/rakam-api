package org.rakam.collection.actor;

import com.google.inject.Injector;
import org.rakam.stream.ActorCacheAdapter;
import org.rakam.database.DatabaseAdapter;
import org.rakam.util.json.JsonObject;

/**
 * Created by buremba <Burak Emre Kabakcı> on 08/11/14 21:04.
 */
public class ActorCollector {

    private final DatabaseAdapter databaseAdapter;
    private final ActorCacheAdapter actorCache;

    public ActorCollector(Injector injector) {
        databaseAdapter = injector.getInstance(DatabaseAdapter.class);
        actorCache = injector.getInstance(ActorCacheAdapter.class);
    }

    public boolean handle(JsonObject json) {
        String project = json.getString("project");
        String actorId = json.getString("id");

        if(project==null || actorId==null) {
            return false;
        }

        JsonObject properties = json.getObject("properties");
        if(properties!=null) {
            actorCache.setActorProperties(project, actorId, properties);
        }

        databaseAdapter.createActor(project, actorId, properties);
        return true;
    }
}
