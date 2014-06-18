package org.rakam.database.cassandra;

import com.datastax.driver.core.*;
import com.google.inject.Inject;
import database.cassandra.CassandraBatchProcessor;
import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;
import org.rakam.ServiceStarter;
import org.rakam.analysis.AnalysisQueryParser;
import org.rakam.analysis.rule.AnalysisRuleList;
import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.rakam.cache.CacheAdapter;
import org.rakam.cache.DistributedAnalysisRuleMap;
import org.rakam.collection.Aggregator;
import org.rakam.database.DatabaseAdapter;
import org.rakam.model.Actor;
import org.rakam.model.Event;
import org.vertx.java.core.json.JsonObject;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by buremba on 21/12/13.
 */

public class CassandraAdapter implements DatabaseAdapter {

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

    final static Logger logger = Logger.getLogger("Cassandra");

    @Inject
    public CassandraAdapter() {
        get_actor_property = session.prepare("select properties from actor where project = ? and id = ? limit 1");
        add_event = session.prepare("insert into event1 (project, actor_id, timestamp, node_id, sequence, data) values (?, ?, ?, ?, ?, ?);");
        create_actor = session.prepare("insert into actor (project, id, properties) values (?, ?, ?);");
        update_actor = session.prepare("update actor set properties = properties + ? where project = ? and id = ? ;");
        get_counter_sql = session.prepare("select value from aggregated_counter where id = ?");
        set_counter_sql = session.prepare("update aggregated_counter set value = value + ? where id = ?");
        get_set_sql = session.prepare("select value from aggregated_set where id = ?");
        get_multi_set_sql = session.prepare("select id, value from aggregated_set where id in ?");
        get_multi_count_sql = session.prepare("select id, value from aggregated_counter where id in ?");
        set_set_sql = session.prepare("update aggregated_set set value = value + ? where id = ?");
        set_aggregation_rules = session.prepare("select * from agg_rules");
        add_aggregation_rule = session.prepare("update agg_rules set rules = rules + ? where project = ?");
        delete_aggregation_rule = session.prepare("update agg_rules set rules = rules - ? where project = ?");
        //batch_filter_range = session.prepare("select timestamp, data, actor_id from event1 where timestamp > ? and timestamp < ? and node_id = ? limit 1000000 allow filtering");
        batch_filter = session.prepare("select timestamp, data, actor_id from event1 where timestamp = ? and node_id = ? limit 1000000 allow filtering");
        batch_filter_range = null;
    }

    @Override
    public void setupDatabase() {
        session.execute("create keyspace analytics WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
        session.execute("use analytics");
        session.execute("create table actor ( project varchar, id varchar, properties map<text, text>, PRIMARY KEY(id, project) );");
        session.execute("create table agg_rules ( project varchar, properties set<text>, PRIMARY KEY(project) );");
        session.execute("create table event ( project varchar, time_cabin int, time timeuuid, actor_id varchar, data map<text, text>, PRIMARY KEY((project, time_cabin), time) );");

    }

    @Override
    public void flushDatabase() {
        session.execute("drop keyspace analytics");
    }

    @Override
    public Actor createActor(String project, String actor_id, Map<String, String> properties) {
        ResultSetFuture future = session.executeAsync(create_actor.bind(project, actor_id, properties));
        try {
            future.get(3, TimeUnit.SECONDS);
            return new Actor(project, actor_id);
        } catch (TimeoutException e) {
            future.cancel(true);
        } catch (ExecutionException e) {
            future.cancel(true);
        } catch (InterruptedException e) {
            future.cancel(true);
        }
        return null;
    }

    @Override
    public void addPropertyToActor(String project, String actor_id, Map<String, String> props) {
        ResultSetFuture future = session.executeAsync(update_actor.bind(props, project, actor_id));
        try {
            future.get(3, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
        } catch (ExecutionException e) {
            future.cancel(true);
        } catch (InterruptedException e) {
            future.cancel(true);
        }
    }

    @Override
    public UUID addEvent(String project, String actor_id, byte[] data) {
        long m = System.currentTimeMillis();
        session.execute(add_event.bind(project, actor_id, (int) m / 1000, ServiceStarter.server_id, (int) ((m % 1000) + 1000 * Thread.currentThread().getId()), ByteBuffer.wrap(data))).one();
        return null;
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
    public void addGroupByItem(String aggregation, String groupBy, String item) {
        addGroupByItem(aggregation, groupBy, item, 1L);
    }

    @Override
    public void addGroupByItem(String aggregation, String groupBy, String item, Long incrementBy) {
        session.execute(set_counter_sql.bind(incrementBy, aggregation + ":" + groupBy + ":" + item));
        Set<String> set = getSet(aggregation + ":" + groupBy + "::keys");
        set.add(item);
        session.execute(set_set_sql.bind(set, aggregation + ":" + groupBy + "::keys"));
    }

    @Override
    public void flush() {

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
        return (a == null) ? new HashSet<String>() : a.getSet("value", String.class);
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
    public Iterator<String> getSetIterator(String key) {
        Row a = session.execute(get_set_sql.bind(key)).one();
        return (a != null) ? a.getSet("value", String.class).iterator() : new Iterator<String>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public String next() {
                return null;
            }

            @Override
            public void remove() {

            }
        };
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
    public Map<String, AnalysisRuleList> getAllRules() {
        List<Row> rows = session.execute(set_aggregation_rules.bind()).all();
        HashMap<String, AnalysisRuleList> map = new HashMap();
        for (Row row : rows) {
            AnalysisRuleList rules = new AnalysisRuleList();
            for (String json_rule : row.getSet("rules", String.class)) {
                try {
                    AnalysisRule rule = AnalysisQueryParser.parse(new JsonObject(json_rule));
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
    public void batch(int start_time, int end_time, int node_id) {
        _batch(session.execute(batch_filter_range.bind(start_time, end_time, node_id)).iterator());
    }

    private void _batch(Iterator<Row> it) {
        Aggregator worker = new Aggregator(this, ServiceStarter.injector.getInstance(CacheAdapter.class), this);
        while (it.hasNext()) {
            Row r = it.next();
            JsonObject event = new JsonObject(new String(r.getBytes("data").array()));
            for (String project : DistributedAnalysisRuleMap.keys())
                worker.aggregate(project, event, r.getString("actor"), (int) (r.getLong("timestamp") / 1000));
        }
    }

    @Override
    public void batch(int start_time, int nodeId) {
        _batch(session.execute(batch_filter.bind(start_time, nodeId)).iterator());
    }

}
