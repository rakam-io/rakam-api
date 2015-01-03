package org.rakam.analysis;

import org.rakam.analysis.query.FilterScript;
import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.rakam.database.ActorDatabase;
import org.rakam.database.AnalysisRuleDatabase;
import org.rakam.database.EventDatabase;
import org.rakam.model.Actor;
import org.rakam.model.Event;
import org.rakam.stream.ActorCacheAdapter;
import org.rakam.util.NotImplementedException;
import org.rakam.util.json.JsonObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Created by buremba <Burak Emre Kabakcı> on 19/09/14 14:00.
 */
public class DummyDatabase implements EventDatabase, ActorCacheAdapter, AnalysisRuleDatabase, ActorDatabase {
    static Map<String, Set<AnalysisRule>> ruleMap = new HashMap();

    static Map<String, Map<String, Actor>> actors = new ConcurrentHashMap<>();
    static Map<String, List<Event>> events = new ConcurrentHashMap();

    @Override
    public Map<String, Set<AnalysisRule>> getAllRules() {
        return ruleMap;
    }

    @Override
    public void add(AnalysisRule rule) {
        ruleMap.computeIfAbsent(rule.project, s -> new ConcurrentSkipListSet<>()).add(rule);
    }

    @Override
    public void delete(AnalysisRule rule) {
        ruleMap.computeIfPresent(rule.project, (s, k) -> {
            k.remove(rule);
            return null;
        });
    }

    @Override
    public Set<AnalysisRule> get(String project) {
        return ruleMap.get(project);
    }

    @Override
    public void clear() {
        ruleMap.clear();
    }

    @Override
    public void setupDatabase() {

    }

    @Override
    public void flushDatabase() {

    }

    @Override
    public void addEvent(String project, String eventName, String actor_id, JsonObject data) {

    }

    @Override
    public Actor createActor(String project, String actor_id, JsonObject properties) {
        Actor value = new Actor(project, actor_id, properties!=null ? properties: null);
        actors.computeIfAbsent(project, k -> new HashMap<>())
                .put(actor_id, value);
        return value;
    }

    @Override
    public void addPropertyToActor(String project, String actor_id, Map<String, Object> props) {
        Map<String, Actor> stringActorMap = actors.get(project);
        if (stringActorMap != null) {
            Actor actor = stringActorMap.get(actor_id);
            if (actor != null) {
                props.forEach((k, v) -> actor.data.put(k, v));
            }
        }
    }

//    @Override
//    public Future addEventAsync(String project, String actor_id, JsonObject data) {
//        return CompletableFuture.supplyAsync(() ->
//                events.computeIfAbsent(project, k -> new LinkedList()).add(new Event(UUID.randomUUID(), project, actor_id, data)));
//    }

    @Override
    public Actor getActor(String project, String actorId) {
        return actors.computeIfAbsent(project, k -> new HashMap<>()).get(actorId);
    }

    @Override
    public Event getEvent(UUID eventId) {
        throw new NotImplementedException();
    }

    @Override
    public void combineActors(String actor1, String actor2) {
        throw new NotImplementedException();
    }

    @Override
    public void processRule(AnalysisRule rule) {
        throw new NotImplementedException();
    }

    @Override
    public Actor[] filterActors(FilterScript filter, int limit, String orderByColumn) {
        ArrayList<Actor> l = new ArrayList();
        actors.forEach((k,v) -> v.forEach((a,b) -> {
            if (filter.test(b.data)) {
                l.add(b);
            }
        }));
        return l.stream().toArray(Actor[]::new);
    }

    @Override
    public Event[] filterEvents(FilterScript filter, int limit, String orderByColumn) {
        ArrayList<Event> l = new ArrayList();
        events.forEach((k,v) -> {
            v.forEach(a -> {
                if (filter.test(a.data)) {
                    l.add(a);
                }
            });
        });
        return l.stream().toArray(Event[]::new);
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
