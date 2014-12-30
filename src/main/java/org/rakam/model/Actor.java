package org.rakam.model;

import org.rakam.util.json.JsonObject;

/**
 * Created by buremba on 21/12/13.
 */
public class Actor implements Entry {
    public final String id;
    public final String project;
    public final JsonObject data;

    public Actor(String project, String actorId) {
        this.id = actorId;
        this.project = project;
        this.data = null;
    }

    public Actor(String projectId, String actorId, String properties) {
        this.id = actorId;
        this.project = projectId;
        this.data = new JsonObject(new String(properties));
        data.putString("id", actorId);
    }

    public Actor(String projectId, String actorId, JsonObject properties) {
        this.id = actorId;
        this.project = projectId;
        this.data = properties;
    }

    public <T> T getAttribute(String attr) {
        return data == null ? null : data.getField(attr);
    }
}