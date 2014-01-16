package org.rakam.database.cassandra;

import com.datastax.driver.core.*;
import org.rakam.database.DatabaseAdapter;
import org.rakam.model.Actor;
import org.rakam.model.Event;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by buremba on 21/12/13.
 */

public class CassandraAdapter extends DatabaseAdapter {
    private Session session = Cluster.builder().addContactPoint("127.0.0.1").build().connect();

    private final PreparedStatement get_actor_property;
    private final PreparedStatement add_event;
    private final PreparedStatement create_actor;

    public CassandraAdapter() {
        session.execute("use analytics");
        get_actor_property = session.prepare("select properties from actor where project = ? and id = ? limit 1");
        add_event = session.prepare("insert into event (project, time_cabin, actor_id, time, data) values (?, ?, ?, now(), ?);");
        create_actor = session.prepare("insert into actor (project, id, properties) values (?, ?, ?);");
    }

    @Override
    public void setupDatabase() {
        session.execute("create keyspace analytics WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
        session.execute("use analytics");
        session.execute("create table actor ( project varchar, id varchar, properties blob, PRIMARY KEY(id, project) );");
        session.execute("create table event ( project varchar, time_cabin int, time timeuuid, actor_id varchar, data blob, PRIMARY KEY((project, time_cabin), time) );");

    }

    public void destroy() {
        session.execute("drop keyspace analytics");
    }

    @Override
    public UUID createActor(String project, String actor_id, byte[] properties) {
        ResultSetFuture future = session.executeAsync(create_actor.bind(project, actor_id, ByteBuffer.wrap(properties)));
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
    public Actor getActor(String project, String actorId) {
        ResultSetFuture future = session.executeAsync(get_actor_property.bind(project, actorId));
        try {
            Row actor = future.get(3, TimeUnit.SECONDS).one();
            return new Actor(project, actorId, actor.getBytes("properties").array());
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
}
