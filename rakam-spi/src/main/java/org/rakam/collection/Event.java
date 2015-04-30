package org.rakam.collection;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import org.apache.avro.generic.GenericRecord;

/**
 * Created by buremba on 21/12/13.
 */
@AutoValue
public abstract class Event {
    @JsonProperty
    public abstract String project();
    @JsonProperty
    public abstract String collection();
    @JsonProperty
    public abstract GenericRecord properties();

    @JsonCreator
    public static Event create(@JsonProperty("project") String project, @JsonProperty("collection") String collection, @JsonProperty("properties") GenericRecord properties) {
        return new AutoValue_Event(project, collection.toLowerCase(), properties);
    }

    public <T> T getAttribute(String attr) {
        return (T) properties().get(attr);
    }
}