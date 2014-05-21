package org.rakam.model;

import org.vertx.java.core.json.JsonObject;

import java.io.Serializable;
import java.util.UUID;

/**
 * Created by buremba on 21/12/13.
 */
public class Event implements Serializable {
    public final UUID id;
    public final String project;
    public final String actor;
    public final JsonObject data;

    public Event(UUID id, String name, String actor, JsonObject data) {
        this.id = id;
        this.project = name;
        this.actor = actor;
        this.data = data;
    }
}