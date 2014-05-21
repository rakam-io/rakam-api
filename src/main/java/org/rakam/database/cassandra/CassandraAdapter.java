package org.rakam.database.cassandra;

import com.datastax.driver.core.*;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.rakam.database.DatabaseAdapter;
import org.rakam.model.Actor;
import org.rakam.model.Event;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by buremba on 21/12/13.
 */

public class CassandraAdapter implements DatabaseAdapter {

    private Session session = Cluster.builder().addContactPoint("127.0.0.1").build().connect();

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


    public CassandraAdapter() {
        session.execute("use analytics");
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
    }

    @Override
    public void setupDatabase() {
        session.execute("create keyspace analytics WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
        session.execute("use analytics");
        session.execute("create table actor ( project varchar, id varchar, properties map<text, text>, PRIMARY KEY(id, project) );");
        session.execute("create table agg_rules ( project varchar, properties set<text>, PRIMARY KEY(project) );");
        session.execute("create table event ( project varchar, time_cabin int, time timeuuid, actor_id varchar, data map<text, text>, PRIMARY KEY((project, time_cabin), time) );");

    }

    public void destroy() {
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

        /*Futures.addCallback(future, new FutureCallback<ResultSet>() {
            @Override
            public void onSuccess(@Nullable com.datastax.driver.core.ResultSet resultSet) {
                // do nothing
            }

            @Override
            public void onFailure(Throwable throwable) {
                System.out.printf("Failed with: %s\n", throwable);
            }
        });*/

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

    public void addRule(String project, String rule) {
        HashSet<String> a = new HashSet();
        a.add(rule);
        session.execute("update agg_rules set rules = rules + ? where project = ?", a, project);
    }

    @Override
    public Actor getActor(String project, String actorId) {
        ResultSetFuture future = session.executeAsync(get_actor_property.bind(project, actorId));
        try {
            Row actor = future.get(3, TimeUnit.SECONDS).one();
            if(actor==null)
                return null;
            Map<String, String> props = actor.getMap("properties", String.class, String.class);
            if (props!=null) {
                return new Actor(project, actorId, props);
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
    public void flushDatabase() {

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
        return session.execute(get_set_sql.bind(key)).one().getSet("value", String.class).size();
    }

    @Override
    public Iterator<String> getSetIterator(String key) {
        return session.execute(get_set_sql.bind(key)).one().getSet("value", String.class).iterator();
    }

    @Override
    public Map<String, Long> getMultiCounts(Collection<String> keys) {
        Iterator<Row> it = session.execute(get_multi_count_sql.bind(keys)).iterator();
        Map<String, Long> l = new HashMap();
        while(it.hasNext()) {
            Row item = it.next();
            l.put(item.getString("id"), item.getLong("value"));
        }
        return l;
    }
}
