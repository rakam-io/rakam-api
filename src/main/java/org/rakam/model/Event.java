package org.rakam.model;

import com.fasterxml.jackson.databind.node.ObjectNode;

import javax.annotation.Nullable;

/**
 * Created by buremba on 21/12/13.
 */
public class Event implements Entry {
    public final String project;
    public final String collection;
    public final @Nullable String user;
    public final ObjectNode properties;

    public Event(String project, String user, String collection, ObjectNode properties) {
        this.project = project;
        this.user = user;
        this.collection = collection;
        this.properties = properties;
    }

    public <T> T getAttribute(String attr) {
        return (T) properties.get(attr);
    }
}