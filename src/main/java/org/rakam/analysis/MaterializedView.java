package org.rakam.analysis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import javax.validation.constraints.NotNull;

/**
 * Created by buremba <Burak Emre Kabakcı> on 08/03/15 00:17.
 */
public class MaterializedView {
    @NotNull
    public final String project;
    @NotNull
    public final String name;
    @NotNull
    public final String query;
    @NotNull
    public final TableStrategy strategy;
    public final String incrementalField;

    @JsonCreator
    public MaterializedView(@JsonProperty("project") String project,
                  @JsonProperty("name") String name,
                  @JsonProperty("query") String query,
                  @JsonProperty("strategy") TableStrategy strategy,
                  @JsonProperty("incrementalField")  String incrementalField) {
        this.project = project;
        this.name = name;
        this.query = query;
        this.incrementalField = incrementalField;
        this.strategy = strategy;
    }
}