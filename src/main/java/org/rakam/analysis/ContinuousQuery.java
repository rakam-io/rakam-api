package org.rakam.analysis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 08/03/15 00:17.
 */
public class ContinuousQuery {
    @NotNull
    public final String project;
    @NotNull
    public final String name;
    @NotNull
    public final String query;
    @NotNull
    public final TableStrategy strategy;
    public final String incrementalField;
    public final List<String> collections;

    @JsonCreator
    public ContinuousQuery(@JsonProperty("project") String project,
                           @JsonProperty("name") String name,
                           @JsonProperty("query") String query,
                           @JsonProperty("strategy") TableStrategy strategy,
                           @JsonProperty("collections") List<String> collections,
                           @JsonProperty("incrementalField") String incrementalField) {
        this.project = project;
        this.name = name;
        this.query = query;
        this.incrementalField = incrementalField;
        this.collections = collections;
        this.strategy = strategy;
    }
}