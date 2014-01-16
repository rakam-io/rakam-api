package org.rakam.model;

import org.vertx.java.core.json.JsonObject;

/**
 * Created by buremba on 21/12/13.
 */
public class Actor {
    String id;
    String project;
    public JsonObject properties;

    public Actor(String project, String actorId, byte[] properties) {
        this.id = actorId;
        this.project = project;
        /*
        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getJsonFactory();
        try {
            this.properties = mapper.readTree(factory.createJsonParser(properties));
        } catch (IOException e) {
            e.printStackTrace();
        } */
        this.properties = new JsonObject(new String(properties));
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
}
