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
    public final JsonObject properties;

    public Actor(String project, String actorId, byte[] properties) {
        this.id = actorId;
        this.project = project;
        this.properties = new JsonObject(new String(properties));
    }
    public Actor(String project, String actorId) {
        this.id = actorId;
        this.project = project;
        this.properties = new JsonObject();
    }

    public Actor(String projectId, String actorId, String properties) {
        this.id = actorId;
        this.project = projectId;
        this.properties = new JsonObject(new String(properties));
    }

    public Actor(String projectId, String actorId, JsonObject properties) {
        this.id = actorId;
        this.project = projectId;
        this.properties = properties;
    }

    public Actor(String project, String actorId, Map<String, String> props) {
        this.id = actorId;
        this.project = project;
        this.properties = JsonHelper.generate(props);
    }
}
