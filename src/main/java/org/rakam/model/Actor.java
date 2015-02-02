package org.rakam.model;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Created by buremba on 21/12/13.
 */
public class Actor implements Entry {
    public final String id;
    public final String project;
    public final ObjectNode data;

    public Actor(String project, String actorId) {
        this.id = actorId;
        this.project = project;
        this.data = null;
    }

    public Actor(String projectId, String actorId, ObjectNode properties) {
        this.id = actorId;
        this.project = projectId;
        this.data = properties;
    }

    public <T> T getAttribute(String attr) {
        return data == null ? null : (T) data.get(attr);
    }
}