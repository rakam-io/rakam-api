package org.rakam.database.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.inject.Inject;
import org.apache.log4j.Logger;
import org.rakam.analysis.query.FilterScript;
import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.rakam.database.ActorDatabase;
import org.rakam.database.EventDatabase;
import org.rakam.model.Actor;
import org.rakam.model.Event;
import org.rakam.util.json.JsonObject;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

/**
 * Created by buremba on 21/12/13.
 */

public class CassandraAdapter implements EventDatabase, ActorDatabase {

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
        add_event = session.prepare("insert into event (project, timestamp, sequence, actor_id, data) values (?, ?, ?, ?, ?);");
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
        batch_filter = session.prepare("select timestamp, data, actor_id from event where project = ? and timestamp = ? and sequence > ? and sequence < ? limit 1000000 allow filtering");
        batch_filter_range = null;
    }

    @Override
    public void setupDatabase() {
        session.execute("create keyspace analytics WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
        session.execute("use analytics");
        session.execute("create table actor ( project varchar, id varchar, created_at timestamp, last_seen timestamp, properties map<text, text>, PRIMARY KEY(id, project) );");
        session.execute("create table agg_rules ( project varchar, properties set<text>, PRIMARY KEY(project) );");
        session.execute("create table agg_rules ( project varchar, properties set<text>, PRIMARY KEY(project) );");
        session.execute("create table event ( project varchar, sequence int, timestamp int, actor_id varchar, data blob, PRIMARY KEY((project, timestamp), sequence);");

    }

    @Override
    public void flushDatabase() {
        session.execute("drop keyspace analytics");
        setupDatabase();
    }

    @Override
    public Actor createActor(String project, String actor_id, JsonObject properties) {
        session.execute(create_actor.bind(project, actor_id, properties));
        return new Actor(project, actor_id, properties);
    }

    @Override
    public void addPropertyToActor(String project, String actor_id, Map<String, Object> props) {
        session.execute(update_actor.bind(props, project, actor_id));
    }

    @Override
    public void addEvent(String project, String eventName, String actor_id, JsonObject data) {
        long m = System.currentTimeMillis();
        try {
//            return session.executeAsync(add_event.bind(project, (int) (m / 1000), (int) ((m % 1000) + (1000000 * ClusterMemberManager.getServerId()) + (1000 * Thread.currentThread().getId())), actor_id, ByteBuffer.wrap(data.encode().getBytes())));
        } catch (Exception e) {
            e.printStackTrace();
        }
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
    public void processRule(AnalysisRule rule) {

    }

    @Override
    public void combineActors(String actor1, String actor2) {

    }
//
//    @Override
//    public void addRule(AnalysisRule rule) {
//        HashSet<String> a = new HashSet();
//        a.add(rule.toJson().encode());
//        session.execute(add_aggregation_rule.bind(a, rule.project));
//    }
//
//    @Override
//    public void deleteRule(AnalysisRule rule) {
//        HashSet<String> a = new HashSet();
//        a.add(rule.toJson().encode());
//        session.execute(add_aggregation_rule.bind(a, rule.project));
//
//        Iterator<Row> rows;
//        LinkedList<String> list;
//
//        rows = session.execute("select id from aggregated_set").iterator();
//        list = new LinkedList();
//        while (rows.hasNext()) {
//            Row row = rows.next();
//            String key = row.getString("id");
//            if (key.startsWith(rule.project))
//                list.add(key);
//        }
//        session.execute("delete from aggregated_set where key in ?", list);
//
//        rows = session.execute("select id from aggregated_counter").iterator();
//        list = new LinkedList();
//        while (rows.hasNext()) {
//            Row row = rows.next();
//            String key = row.getString("id");
//            if (key.startsWith(rule.project))
//                list.add(key);
//        }
//        session.execute("delete from aggregated_counter where key in ?", list);
//    }

//    @Override
//    public void processRule(AnalysisRule rule, long start_time, long end_time) {
//        CassandraBatchProcessor.processRule(rule, start_time, end_time);
//    }
//
//    @Override
//    public Map<String, Set<AnalysisRule>> getAllRules() {
//        List<Row> rows = session.execute(set_aggregation_rules.bind()).all();
//        HashMap<String, Set<AnalysisRule>> map = new HashMap();
//        for (Row row : rows) {
//            HashSet<AnalysisRule> rules = new HashSet();
//            for (String json_rule : row.getSet("rules", String.class)) {
//                try {
//                    JsonObject json = new JsonObject(json_rule);
//                    AnalysisRule rule = AnalysisRuleParser.parse(json);
//                    rules.add(rule);
//                } catch (IllegalArgumentException e) {
//                    logger.error("analysis rule couldn't parsed: " + json_rule, e);
//                }
//            }
//
//            map.put(row.getString("project"), rules);
//        }
//        return map;
//    }

//    @Override
//    public void batch(String project, int start_time, int end_time, int node_id) {
//        _batch(session.execute(batch_filter_range.bind(start_time, end_time, node_id)).iterator());
//    }

    private void _batch(Iterator<Row> it) {
//        EventAggregator worker = new EventAggregator(this, ServiceStarter.injector.getInstance(CacheAdapter.class), this);
        while (it.hasNext()) {
            Row r = it.next();
            JsonObject event = new JsonObject(new String(r.getBytes("data").array()));
//            for (String project : keys())
//                worker.aggregate(project, event, r.getString("actor"), (int) (r.getLong("timestamp") / 1000));
        }
    }

//    @Override
//    public void batch(String project, int start_time, int nodeId) {
//        _batch(session.execute(batch_filter.bind(project, start_time, nodeId)).iterator());
//    }

    @Override
    public Actor[] filterActors(FilterScript filter, int limit, String orderByColumn) {
//        return CassandraBatchProcessor.filterActors(filter, limit, orderByColumn);
        return null;
    }

    @Override
    public Event[] filterEvents(FilterScript filter, int limit, String orderByColumn) {
//        return CassandraBatchProcessor.filterEvents(filter, limit, orderByColumn);
        return null;
    }

}
