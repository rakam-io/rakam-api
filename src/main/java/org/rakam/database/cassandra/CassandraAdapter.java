package org.rakam.database.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.inject.Inject;
import database.cassandra.CassandraBatchProcessor;
import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;
import org.rakam.ServiceStarter;
import org.rakam.analysis.AnalysisRuleParser;
import org.rakam.analysis.AverageCounter;
import org.rakam.analysis.query.FilterScript;
import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.rakam.cache.CacheAdapter;
import org.rakam.cache.hazelcast.hyperloglog.HLLWrapper;
import org.rakam.cluster.MemberShipListener;
import org.rakam.collection.EventAggregator;
import org.rakam.database.AnalysisRuleDatabase;
import org.rakam.database.DatabaseAdapter;
import org.rakam.model.Actor;
import org.rakam.model.Event;
import org.rakam.util.HLLWrapperImpl;
import org.vertx.java.core.json.JsonObject;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.rakam.cache.DistributedAnalysisRuleMap.keys;
import static org.rakam.util.DateUtil.UTCTime;

/**
 * Created by buremba on 21/12/13.
 */

public class CassandraAdapter implements DatabaseAdapter, CacheAdapter, AnalysisRuleDatabase {

    private Session session = Cluster.builder().addContactPoint("127.0.0.1").build().connect("analytics");

    private final PreparedStatement get_actor_property;
    private final PreparedStatement add_event;
    private final PreparedStatement create_actor;
    private final PreparedStatement update_actor;
    private final PreparedStatement set_set_sql;
    private final PreparedStatement get_counter_sql;
    private final PreparedStatement set_counter_sql;
    private final PreparedStatement get_set_sql;
    private final PreparedStatement get_multi_set_sql;
    private final PreparedStatement get_multi_count_sql;
    private final PreparedStatement set_aggregation_rules;
    private final PreparedStatement add_aggregation_rule;
    private final PreparedStatement delete_aggregation_rule;
    private final PreparedStatement batch_filter_range;
    private final PreparedStatement batch_filter;

    final static Logger logger = Logger.getLogger(CassandraAdapter.class.getName());

    @Inject
    public CassandraAdapter() {
        get_actor_property = session.prepare("select properties from actor where project = ? and id = ? limit 1");
        add_event = session.prepare("insert into event (project, actor_id, timestamp, node_id, sequence, data) values (?, ?, ?, ?, ?, ?);");
        create_actor = session.prepare("insert into actor (project, id, properties) values (?, ?, ?);");
        update_actor = session.prepare("update actor set properties = properties + ? where project = ? and id = ?;");
        get_counter_sql = session.prepare("select value from aggregated_counter where id = ?");
        set_counter_sql = session.prepare("update aggregated_counter set value = value + ? where id = ?");
        get_set_sql = session.prepare("select value from aggregated_set where id = ?");
        get_multi_set_sql = session.prepare("select id, value from aggregated_set where id in ?");
        get_multi_count_sql = session.prepare("select id, value from aggregated_counter where id in ?");
        set_set_sql = session.prepare("update aggregated_set set value = value + ? where id = ?");
        set_aggregation_rules = session.prepare("select * from agg_rules");
        add_aggregation_rule = session.prepare("update agg_rules set rules = rules + ? where project = ?");
        delete_aggregation_rule = session.prepare("update agg_rules set rules = rules - ? where project = ?");
        //batch_filter_range = session.prepare("select timestamp, data, actor_id from event where timestamp > ? and timestamp < ? and node_id = ? limit 1000000 allow filtering");
        batch_filter = session.prepare("select timestamp, data, actor_id from event where project = ? and node_id = ? and timestamp = ? limit 1000000 allow filtering");
        batch_filter_range = null;
    }

    @Override
    public void setupDatabase() {
        session.execute("create keyspace analytics WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
        session.execute("use analytics");
        session.execute("create table actor ( project varchar, id varchar, created_at timestamp, last_seen timestamp, properties map<text, text>, PRIMARY KEY(id, project) );");
        session.execute("create table agg_rules ( project varchar, properties set<text>, PRIMARY KEY(project) );");
        session.execute("create table agg_rules ( project varchar, properties set<text>, PRIMARY KEY(project) );");
        session.execute("create table event ( project varchar, node_id int, sequence int, timestamp int, actor_id varchar, data blob, PRIMARY KEY((project, node_id, sequence) );");

    }

    @Override
    public void flushDatabase() {
        session.execute("drop keyspace analytics");
        setupDatabase();
    }

    @Override
    public Actor createActor(String project, String actor_id, Map<String, Object> properties) {
        session.execute(create_actor.bind(project, actor_id, properties));
        return new Actor(project, actor_id, new JsonObject(properties));
    }

    @Override
    public void addPropertyToActor(String project, String actor_id, Map<String, Object> props) {
        session.execute(update_actor.bind(props, project, actor_id));
    }

    @Override
    public void addEvent(String project, String actor_id, JsonObject data) {
        int m = UTCTime();
        session.execute(add_event.bind(project, actor_id, m, MemberShipListener.getServerId(), (int) ((m % 1000) + 1000 * Thread.currentThread().getId()), ByteBuffer.wrap(data.encode().getBytes()))).one();
    }

    @Override
    public Actor getActor(String project, String actorId) {
        Row actor = session.execute(get_actor_property.bind(project, actorId)).one();
        if (actor == null)
            return null;
        Map<String, Object> props = actor.getMap("properties", String.class, Object.class);
        if (props != null) {
            return new Actor(project, actorId, new JsonObject(props));
        } else
            return new Actor(project, actorId);

    }

    @Override
    public Event getEvent(UUID event) {
        return null;
    }

    @Override
    public void combineActors(String actor1, String actor2) {

    }

    @Override
    public void addSet(String key, String item) {
        HashSet a = new HashSet<String>();
        a.add(item);
        session.execute(set_set_sql.bind(a, key));
    }

    @Override
    public void removeSet(String setName) {

    }

    @Override
    public void removeCounter(String setName) {

    }

    @Override
    public void addSet(String key, Collection<String> items) {
        session.execute(set_set_sql.bind(items, key));
    }

    @Override
    public void incrementGroupBySimpleCounter(String id, String groupBy, long incrementBy) {
        session.execute(set_counter_sql.bind(incrementBy, id + ":" + groupBy));
        final Set<String> set1 = getSet(id + "::keys");
        Set<String> set = set1==null ? new HashSet() : new HashSet(set1);
        set.add(groupBy);
        session.execute(set_set_sql.bind(set, id + "::keys"));
    }

    @Override
    public void setGroupBySimpleCounter(String aggregation, String groupBy, long l) {
        setCounter(aggregation+":"+groupBy, l);
    }

    @Override
    public Long getGroupBySimpleCounter(String aggregation, String groupBy) {
        return getCounter(aggregation+":"+groupBy);
    }

    @Override
    public void flush() {
        throw new NotImplementedException();
    }

    @Override
    public void addGroupByString(String id, String groupByValue, String type_target) {
        addSet(id + ":" + groupByValue, type_target);
        addSet(id + "::keys", groupByValue);
    }

    @Override
    public void addGroupByString(String id, String groupByValue, Collection<String> s) {
        addSet(id + ":" + groupByValue, s);
        addSet(id + "::keys", groupByValue);
    }

    @Override
    public void removeGroupByCounters(String key) {
        for(String item : getSet(key + "::keys")) {
            removeCounter(key + ":" + item);
        }
        removeSet(key + "::keys");
    }

    @Override
    public Map<String, Long> getGroupByCounters(String key) {
        HashMap<String, Long> map = new HashMap<>();
        for(String item : getSet(key + "::keys")) {
            map.put(item, getCounter(key + ":" + item));
        }
        return map;
    }

    @Override
    public Map<String, Set<String>> getGroupByStrings(String key) {
        HashMap<String, Set<String>> map = new HashMap<>();
        for(String item : getSet(key + "::keys")) {
            map.put(item, getSet(key + ":" + item));
        }
        return map;
    }

    @Override
    public Map<String, Set<String>> getGroupByStrings(String key, int limit) {
        HashMap<String, Set<String>> map = new HashMap<>();
        Iterator<String> set = getSet(key + "::keys").iterator();
        int i = 0;
        while(i++<limit && set.hasNext()) {
            String item = set.next();
            map.put(item, getSet(key + ":" + item));
        }
        return map;
    }

    @Override
    public Map<String, HLLWrapper> estimateGroupByStrings(String key, int limit) {
        // TODO: we will change this implementation to new Cassandra custom functions feature when Cassandra 3 is released
        Map<String, HLLWrapper> map = new HashMap<>();
        Iterator<String> set = getSet(key + "::keys").iterator();
        int i = 0;
        while(i++<limit && set.hasNext()) {
            String item = set.next();
            HLLWrapperImpl hllWrapper = new HLLWrapperImpl();
            getSet(key + ":" + item).forEach(hllWrapper::add);
            map.put(item, hllWrapper);
        }
        return map;
    }

    @Override
    public Map<String, Long> getGroupByCounters(String key, int limit) {
        HashMap<String, Long> map = new HashMap<>();
        final Set<String> set = getSet(key + "::keys");
        if (set != null) {
            Iterator<String> iterator = set.iterator();
            int i = 0;
            while(i++<limit && iterator.hasNext()) {
                String item = iterator.next();
                map.put(item, getCounter(key+":"+item));
            }
            return map;
        }else {
            return null;
        }
    }

    @Override
    public void incrementGroupByAverageCounter(String id, String key, long sum, long counter) {
        throw new NotImplementedException();
    }

    @Override
    public void incrementAverageCounter(String id, long sum, long counter) {
        incrementCounter(id+":sum", sum);
        incrementCounter(id + ":count", counter);
    }

    @Override
    public AverageCounter getAverageCounter(String id) {
        return new AverageCounter(getCounter(id+":sum"), getCounter(id + ":count"));
    }

    @Override
    public void removeGroupByStrings(String key) {
        for(String item : getSet(key + "::keys")) {
            removeSet(key + ":" + item);
        }
        removeSet(key + "::keys");
    }

    @Override
    public Map<String, Long> getGroupByStringsCounts(String key, Integer limit) {
        HashMap<String, Long> map = new HashMap<>();
        Iterator<String> set = getSet(key + "::keys").iterator();
        int i = 0;
        while(i++<limit && set.hasNext()) {
            String item = set.next();
            map.put(item, (long) getSet(key + ":" + item).size());
        }
        return map;
    }

    @Override
    public void incrementCounter(String key) {
        session.execute(set_counter_sql.bind(1, key));
    }

    @Override
    public Long getCounter(String key) {
        Row a = session.execute(get_counter_sql.bind(key)).one();
        return (a == null) ? 0 : a.getLong("value");
    }

    @Override
    public Set<String> getSet(String key) {
        Row a = session.execute(get_set_sql.bind(key)).one();
        return (a == null) ? null : a.getSet("value", String.class);
    }

    @Override
    public void incrementCounter(String key, long incrementBy) {
        session.execute(set_counter_sql.bind(incrementBy, key));
    }

    @Override
    public void setCounter(String s, long target) {
        throw new NotImplementedException();
    }

    @Override
    public int getSetCount(String key) {
        Row a = session.execute(get_set_sql.bind(key)).one();
        return (a == null) ? 0 : a.getSet("value", String.class).size();
    }


    @Override
    public Map<String, Long> getCounters(Collection<String> keys) {
        Iterator<Row> it = session.execute(get_multi_count_sql.bind(keys)).iterator();
        Map<String, Long> l = new HashMap();
        while (it.hasNext()) {
            Row item = it.next();
            l.put(item.getString("id"), item.getLong("value"));
        }
        return l;
    }

    @Override
    public void addRule(AnalysisRule rule) {
        HashSet<String> a = new HashSet();
        a.add(rule.toJson().encode());
        session.execute(add_aggregation_rule.bind(a, rule.project));
    }

    @Override
    public void deleteRule(AnalysisRule rule) {
        HashSet<String> a = new HashSet();
        a.add(rule.toJson().encode());
        session.execute(add_aggregation_rule.bind(a, rule.project));

        Iterator<Row> rows;
        LinkedList<String> list;

        rows = session.execute("select id from aggregated_set").iterator();
        list = new LinkedList();
        while (rows.hasNext()) {
            Row row = rows.next();
            String key = row.getString("id");
            if (key.startsWith(rule.project))
                list.add(key);
        }
        session.execute("delete from aggregated_set where key in ?", list);

        rows = session.execute("select id from aggregated_counter").iterator();
        list = new LinkedList();
        while (rows.hasNext()) {
            Row row = rows.next();
            String key = row.getString("id");
            if (key.startsWith(rule.project))
                list.add(key);
        }
        session.execute("delete from aggregated_counter where key in ?", list);
    }

    @Override
    public void processRule(AnalysisRule rule) {
        CassandraBatchProcessor.processRule(rule);
    }

    @Override
    public void processRule(AnalysisRule rule, long start_time, long end_time) {
        CassandraBatchProcessor.processRule(rule, start_time, end_time);
    }

    @Override
    public Map<String, Set<AnalysisRule>> getAllRules() {
        List<Row> rows = session.execute(set_aggregation_rules.bind()).all();
        HashMap<String, Set<AnalysisRule>> map = new HashMap();
        for (Row row : rows) {
            HashSet<AnalysisRule> rules = new HashSet();
            for (String json_rule : row.getSet("rules", String.class)) {
                try {
                    JsonObject json = new JsonObject(json_rule);
                    AnalysisRule rule = AnalysisRuleParser.parse(json);
                    rules.add(rule);
                } catch (IllegalArgumentException e) {
                    logger.error("analysis rule couldn't parsed: " + json_rule, e);
                }
            }

            map.put(row.getString("project"), rules);
        }
        return map;
    }

    @Override
    public void batch(String project, int start_time, int end_time, int node_id) {
        _batch(session.execute(batch_filter_range.bind(start_time, end_time, node_id)).iterator());
    }

    private void _batch(Iterator<Row> it) {
        EventAggregator worker = new EventAggregator(this, ServiceStarter.injector.getInstance(CacheAdapter.class), this);
        while (it.hasNext()) {
            Row r = it.next();
            JsonObject event = new JsonObject(new String(r.getBytes("data").array()));
            for (String project : keys())
                worker.aggregate(project, event, r.getString("actor"), (int) (r.getLong("timestamp") / 1000));
        }
    }

    @Override
    public void batch(String project, int start_time, int nodeId) {
        _batch(session.execute(batch_filter.bind(project, start_time, nodeId)).iterator());
    }

    @Override
    public Actor[] filterActors(FilterScript filter, int limit, String orderByColumn) {
        return CassandraBatchProcessor.filterActors(filter, limit, orderByColumn);
    }

    @Override
    public Event[] filterEvents(FilterScript filter, int limit, String orderByColumn) {
        return CassandraBatchProcessor.filterEvents(filter, limit, orderByColumn);
    }

    @Override
    public HLLWrapper createHLLFromSets(String... keys) {
        // TODO: we will change this implementation to new Cassandra custom functions feature when Cassandra 3 is released
        HLLWrapperImpl hllWrapper = new HLLWrapperImpl();
        for (String key : keys) {
            final Set<String> set = getSet(key);
            if(set!=null)
                set.forEach(hllWrapper::add);
        }
        return hllWrapper;
    }

    @Override
    public Map<String, AverageCounter> getGroupByAverageCounters(String rule_id) {
        return getGroupByAverageCounters(rule_id, Integer.MAX_VALUE);
    }

    @Override
    public Map<String, AverageCounter> getGroupByAverageCounters(String key, int limit) {
        Set<String> set = getSet(key + "::keys");
        HashMap<String, AverageCounter> stringLongHashMap = new HashMap<>(set.size());
        if(set==null) return null;

        Iterator<String> it = set.iterator();
        int i = 0;
        while(i++<limit && it.hasNext()) {
            String item = it.next();
            stringLongHashMap.put(item, getAverageCounter(key + ":" + item));
        }
        return stringLongHashMap;
    }
}
