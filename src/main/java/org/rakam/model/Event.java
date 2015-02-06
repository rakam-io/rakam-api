package org.rakam.model;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;

/**
 * Created by buremba on 21/12/13.
 */
@AutoValue
public abstract class Event implements Entry {
    public abstract String project();
    public abstract String collection();
    public abstract @Nullable String user();
    public abstract ObjectNode properties();

    protected Event() {
    }

    public static Event create(String project, String user, String collection, ObjectNode properties) {
        return new AutoValue_Event(project, user, collection, properties);
    }

    public <T> T getAttribute(String attr) {
        return (T) properties().get(attr);
    }
}