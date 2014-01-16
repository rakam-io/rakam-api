package org.rakam.database;

import org.rakam.model.Actor;
import org.rakam.model.Event;

import java.util.UUID;

/**
 * Created by buremba on 21/12/13.
 */
public abstract class DatabaseAdapter {

    public abstract void setupDatabase();
    public abstract void destroy();

    public abstract UUID createActor(String project, String actor_id, byte[] properties);


    public abstract UUID addEvent(String project, int time_cabin, String actor_id, byte[] data);


    public abstract Actor getActor(String project, String actorId);

    public abstract Event getEvent(UUID eventId);
    public abstract void combineActors(String actor1, String actor2);
    public abstract void flushDatabase();

}
