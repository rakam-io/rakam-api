package org.rakam.collection;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.avro.generic.GenericRecord;


public class Event {

    private final String project;
    private final String collection;
    private final GenericRecord properties;

    @JsonCreator
    public Event(@JsonProperty("project") String project, @JsonProperty("collection") String collection, @JsonProperty("properties") GenericRecord properties) {
        this.project = project;
        this.collection = collection;
        this.properties = properties;
    }

    @JsonProperty
    public String project() {
        return project;
    }

    @JsonProperty
    public String collection() {
        return collection;
    }

    @JsonProperty
    public GenericRecord properties() {
        return properties;
    }

    public <T> T getAttribute(String attr) {
        return (T) properties().get(attr);
    }
}