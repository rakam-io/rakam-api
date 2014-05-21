package org.rakam.model;

import org.rakam.util.JsonHelper;
import org.vertx.java.core.json.JsonObject;

import java.util.Map;

/**
 * Created by buremba on 21/12/13.
 */
public class Actor {
    public final String id;
    public final String project;
    public final JsonObject data;

    public Actor(String project, String actorId, byte[] data) {
        this.id = actorId;
        this.project = project;
        this.data = new JsonObject(new String(data));
    }
    public Actor(String project, String actorId) {
        this.id = actorId;
        this.project = project;
        this.data = null;
    }

    public Actor(String projectId, String actorId, String properties) {
        this.id = actorId;
        this.project = projectId;
        this.data = new JsonObject(new String(properties));
    }

    public Actor(String projectId, String actorId, JsonObject properties) {
        this.id = actorId;
        this.project = projectId;
        this.data = properties;
    }

    public Actor(String project, String actorId, Map<String, String> props) {
        this.id = actorId;
        this.project = project;
        this.data = JsonHelper.generate(props);
    }
}