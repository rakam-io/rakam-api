package org.rakam.database.elasticsearch;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.rakam.analysis.query.FilterScript;
import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.rakam.analysis.AverageCounter;
import org.rakam.database.DatabaseAdapter;
import org.rakam.model.Actor;
import org.rakam.model.Event;
import org.vertx.java.core.json.JsonObject;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/07/14 06:58.
 */
public class ElasticSearchAdapter implements DatabaseAdapter {
    private final Client client = new TransportClient()
            .addTransportAddress(new InetSocketTransportAddress("127.0.0.1", 9300));
    @Override
    public void setupDatabase() {

    }

    @Override
    public void flushDatabase() {

    }

    @Override
    public Actor createActor(String project, String actor_id, Map<String, Object> properties) {
        return null;
    }

    @Override
    public void addPropertyToActor(String project, String actor_id, Map<String, Object> props) {

    }

    @Override
    public void addEvent(String project, String actor_id, JsonObject data) {
       client.prepareIndex("rakam", "event").setSource(data.encode()).execute()
               .actionGet();
    }

    @Override
    public Actor getActor(String project, String actorId) {
        return null;
    }

    @Override
    public Event getEvent(UUID eventId) {
        return null;
    }

    @Override
    public void combineActors(String actor1, String actor2) {

    }

    @Override
    public void incrementCounter(String key, long increment) {

    }

    @Override
    public void setCounter(String s, long target) {

    }

    @Override
    public void addSet(String setName, String item) {

    }

    @Override
    public void removeSet(String setName) {

    }

    @Override
    public void removeCounter(String setName) {

    }

    @Override
    public long getCounter(String key) {
        return 0;
    }

    @Override
    public int getSetCount(String key) {
        return 0;
    }

    @Override
    public Set<String> getSet(String key) {
        return null;
    }

    @Override
    public void incrementCounter(String key) {

    }

    @Override
    public void addSet(String setName, Collection<String> items) {

    }

    @Override
    public void flush() {

    }

    @Override
    public Map<String, Long> getCounters(Collection<String> keys) {
        return null;
    }

    @Override
    public void processRule(AnalysisRule rule) {

    }

    @Override
    public void processRule(AnalysisRule rule, long start_time, long end_time) {

    }

    @Override
    public void batch(String project, int start_time, int end_time, int nodeId) {

    }

    @Override
    public void batch(String project, int start_time, int nodeId) {

    }

    @Override
    public Actor[] filterActors(FilterScript filter, int limit, String orderByColumn) {
        return new Actor[0];
    }

    @Override
    public Event[] filterEvents(FilterScript filter, int limit, String orderByColumn) {
        return new Event[0];
    }

    @Override
    public void incrementGroupBySimpleCounter(String aggregation, String groupBy, long incrementBy) {

    }

    @Override
    public void addGroupByString(String id, String groupByValue, String s) {

    }

    @Override
    public void addGroupByString(String id, String groupByValue, Collection<String> s) {

    }

    @Override
    public void removeGroupByCounters(String key) {

    }

    @Override
    public Map<String, Long> getGroupByCounters(String key) {
        return null;
    }

    @Override
    public Map<String, Set<String>> getGroupByStrings(String key) {
        return null;
    }

    @Override
    public Map<String, Set<String>> getGroupByStrings(String key, int limit) {
        return null;
    }

    @Override
    public Map<String, Long> getGroupByCounters(String key, int limit) {
        return null;
    }

    @Override
    public void incrementGroupByAverageCounter(String id, String key, long sum, long counter) {

    }

    @Override
    public void incrementAverageCounter(String id, long sum, long counter) {

    }

    @Override
    public AverageCounter getAverageCounter(String id) {
        return null;
    }

    @Override
    public void removeGroupByStrings(String key) {

    }

    @Override
    public Map<String, Long> getGroupByStringsCounts(String key, Integer items) {
        return null;
    }

    @Override
    public Map<String, AverageCounter> getGroupByAverageCounters(String rule_id) {
        return null;
    }

    @Override
    public Map<String, AverageCounter> getGroupByAverageCounters(String key, int limit) {
        return null;
    }
}
