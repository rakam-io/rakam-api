package org.rakam.database.cassandra;

import com.datastax.driver.core.*;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import org.apache.log4j.Logger;
import org.rakam.analysis.AnalysisQueryParser;
import org.rakam.analysis.rule.AnalysisRuleList;
import org.rakam.analysis.rule.aggregation.AnalysisRule;
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

    final static Logger logger = Logger.getLogger("Cassandra");

    @Inject
    public CassandraAdapter() {
        get_actor_property = session.prepare("select properties from actor where project = ? and id = ? limit 1");
        add_event = session.prepare("insert into event (project, time_cabin, actor_id, time, data) values (?, ?, ?, now(), ?);");
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
    public Actor createActor(String project, String actor_id, Map<String,String> properties) {
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
    public UUID addEvent(String project, int time_cabin, String actor_id, byte[] data) {
        ResultSetFuture future = session.executeAsync(add_event.bind(project, time_cabin, actor_id, ByteBuffer.wrap(data)));
        try {
            future.get(3, TimeUnit.SECONDS);
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
    public void addEventAsync(String project, int time_cabin, String actor_id, byte[] data) {
        ResultSetFuture future = session.executeAsync(add_event.bind(project, time_cabin, actor_id, ByteBuffer.wrap(data)));
        Futures.addCallback(future, new FutureCallback<ResultSet>() {
            @Override
            public void onSuccess(ResultSet resultSet) {}

            @Override
            public void onFailure(Throwable throwable) {
                System.out.printf("Failed with: %s\n", throwable);
            }
        });
    }

    @Override
    public Actor getActor(String project, String actorId) {
        ResultSetFuture future = session.executeAsync(get_actor_property.bind(project, actorId));
        try {
            Row actor = future.get(3, TimeUnit.SECONDS).one();
            if(actor==null)
                return null;
            Map<String, Object> props = actor.getMap("properties", String.class, Object.class);
            if (props!=null) {
                return new Actor(project, actorId, new JsonObject(props));
            } else
                return new Actor(project, actorId);
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
    public void addSet(String key, Set<String> items) {
        session.execute(set_set_sql.bind(items, key));
    }

    @Override
    public void incrementCounter(String key) {
        session.execute(set_counter_sql.bind(1, key));
    }

    @Override
    public long getCounter(String key) {
        Row a = session.execute(get_counter_sql.bind(key)).one();
        return (a==null) ? 0 : a.getLong("value");
    }

    @Override
    public Set<String> getSet(String key) {
        Row a = session.execute(get_set_sql.bind(key)).one();
        return (a==null) ? new HashSet<String>() : a.getSet("value", String.class);
    }

    @Override
    public void incrementCounter(String key, long incrementBy) {
        session.execute(set_counter_sql.bind(incrementBy, key));
    }

    @Override
    public int getSetCount(String key) {
        Row a = session.execute(get_set_sql.bind(key)).one();
        return (a==null) ? 0 : a.getSet("value", String.class).size();
    }

    @Override
    public Iterator<String> getSetIterator(String key) {
        Row a = session.execute(get_set_sql.bind(key)).one();
        return (a!=null) ? a.getSet("value", String.class).iterator() : new Iterator<String>() {
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
        while(it.hasNext()) {
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
        while(rows.hasNext()) {
            Row row = rows.next();
            String key = row.getString("id");
            if(key.startsWith(rule.project))
                list.add(key);
        }
        session.execute("delete from aggregated_set where key in ?", list);

        rows = session.execute("select id from aggregated_counter").iterator();
        list = new LinkedList();
        while(rows.hasNext()) {
            Row row = rows.next();
            String key = row.getString("id");
            if(key.startsWith(rule.project))
                list.add(key);
        }
        session.execute("delete from aggregated_counter where key in ?", list);
    }

    @Override
    public Map<String, AnalysisRuleList> getAllRules() {
        List<Row> rows = session.execute(set_aggregation_rules.bind()).all();
        HashMap<String, AnalysisRuleList> map = new HashMap();
        for(Row row : rows) {
            AnalysisRuleList rules = new AnalysisRuleList();
            for(String json_rule : row.getSet("rules", String.class)) {
                try {
                    AnalysisRule rule = AnalysisQueryParser.parse(new JsonObject(json_rule));
                    rules.add(rule);
                } catch (IllegalArgumentException e) {
                    logger.error("analysis rule couldn't parsed: "+json_rule, e);
                }
            }

            map.put(row.getString("project"), rules);
        }
        return map;
    }
}
