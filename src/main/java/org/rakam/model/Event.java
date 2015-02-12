package org.rakam.model;

import com.google.auto.value.AutoValue;
import org.apache.avro.generic.GenericData;

/**
 * Created by buremba on 21/12/13.
 */
@AutoValue
public abstract class Event implements Entry {
    public abstract String project();
    public abstract String collection();
    public abstract GenericData.Record properties();

    protected Event() {
    }


    public static Event create(String project, String collection, GenericData.Record properties) {
        return new AutoValue_Event(project, collection.toLowerCase(), properties);
    }

    public <T> T getAttribute(String attr) {
        return (T) properties().get(attr);
    }
}