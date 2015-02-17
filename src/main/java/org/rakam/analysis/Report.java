package org.rakam.analysis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/02/15 22:03.
 */
public class Report {
    public final String name;
    public final String query;

    public final ReportStrategy strategy;

    @JsonCreator
    public Report(@JsonProperty("name") String name,
                  @JsonProperty("query") String query,
                  @JsonProperty("strategy") ReportStrategy strategy) {
        this.name = name;
        this.query = query;
        this.strategy = strategy;
    }
}
