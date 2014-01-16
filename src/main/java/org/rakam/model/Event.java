package org.rakam.model;

import org.vertx.java.core.json.JsonObject;

import java.util.UUID;

/**
 * Created by buremba on 21/12/13.
 */
public class Event {
    UUID id;
    JsonObject data;

    public Event(UUID id, JsonObject data) {
        this.id = id;
        this.data = data;
    }
}
