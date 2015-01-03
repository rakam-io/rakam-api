package org.rakam.database.rakamdb;

import com.google.inject.Inject;
import org.rakam.analysis.query.FilterScript;
import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.rakam.database.ActorDatabase;
import org.rakam.database.EventDatabase;
import org.rakam.kume.Cluster;
import org.rakam.model.Actor;
import org.rakam.model.Event;
import org.rakam.util.Interval;
import org.rakam.util.json.JsonObject;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/01/15 14:12.
 */
public class DefaultDatabaseAdapter implements EventDatabase, ActorDatabase {
    private final Cluster cluster;
    Map<String, RakamDB> dbs = new ConcurrentHashMap<>();

    @Inject
    public DefaultDatabaseAdapter(Cluster cluster) {
        this.cluster = cluster;
    }


    RakamDB getDBforProject(String projectId) {
        RakamDB rakamDB = dbs.get(projectId);
        if(rakamDB==null) {
            RakamDB db = cluster.createOrGetService("rakamdb_" + projectId, bus -> new RakamDB(bus, Interval.MINUTE, 2));
            dbs.put(projectId, db);
            return db;
        } else {
            return rakamDB;
        }
    }

    @Override
    public void setupDatabase() {

    }

    @Override
    public void flushDatabase() {

    }

    @Override
    public void addEvent(String project, String eventName, String actor_id, JsonObject data) {
        if(actor_id!=null)
            data.put("actor", actor_id);
        getDBforProject(project).addEvent(eventName, data);
    }

    @Override
    public Event getEvent(UUID eventId) {
        return null;
    }

    @Override
    public void processRule(AnalysisRule rule) {

    }

    @Override
    public Event[] filterEvents(FilterScript filter, int limit, String orderByColumn) {
        return new Event[0];
    }

    @Override
    public Actor createActor(String project, String actor_id, JsonObject properties) {
        return null;
    }

    @Override
    public Actor getActor(String project, String actorId) {
        return null;
    }

    @Override
    public void addPropertyToActor(String project, String actor_id, Map<String, Object> props) {

    }

    @Override
    public void combineActors(String actor1, String actor2) {

    }

    @Override
    public Actor[] filterActors(FilterScript filter, int limit, String orderByColumn) {
        return new Actor[0];
    }
}
