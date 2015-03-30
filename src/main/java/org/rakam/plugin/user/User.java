package org.rakam.plugin.user;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * Created by buremba <Burak Emre Kabakcı> on 29/03/15 23:10.
 */
public class User {
    public final String project;
    public final Object id;
    public final Map<String, Object> properties;

    @JsonCreator
    public User(@JsonProperty("project") String project,
                @JsonProperty("id") Object id,
                @JsonProperty("properties") Map<String, Object> properties) {
        this.project = project;
        this.id = id;
        this.properties = properties;
    }
}
