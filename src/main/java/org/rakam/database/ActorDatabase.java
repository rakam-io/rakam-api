package org.rakam.database;

import org.rakam.analysis.query.FilterScript;
import org.rakam.model.Actor;
import org.rakam.util.json.JsonObject;

import java.util.Map;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/01/15 14:17.
 */
public interface ActorDatabase {
    Actor createActor(String project, String actor_id, JsonObject properties);
    Actor getActor(String project, String actorId);
    void addPropertyToActor(String project, String actor_id, Map<String, Object> props);
    void combineActors(String actor1, String actor2);
    public Actor[] filterActors(FilterScript filter, int limit, String orderByColumn);
}
