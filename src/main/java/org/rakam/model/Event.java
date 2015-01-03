package org.rakam.model;

import org.rakam.util.json.JsonObject;

import java.util.UUID;

/**
 * Created by buremba on 21/12/13.
 */
public class Event implements Entry {
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

    public <T> T getAttribute(String attr) {
        return (T) data.getValue(attr);
    }
}