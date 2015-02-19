package org.rakam.analysis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import javax.validation.constraints.NotNull;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/02/15 22:03.
 */
public class Report {
    @NotNull
    public final String project;
    @NotNull
    public final String name;
    @NotNull
    public final String query;
    @NotNull
    public final ReportStrategy strategy;
    public final JsonNode options;

    @JsonCreator
    public Report(@JsonProperty("project") String project,
                  @JsonProperty("name") String name,
                  @JsonProperty("query") String query,
                  @JsonProperty("strategy") ReportStrategy strategy,
                  @JsonProperty("options")  JsonNode options) {
        this.project = project;
        this.name = name;
        this.query = query;
        this.options = options;
        this.strategy = strategy;
    }
}
