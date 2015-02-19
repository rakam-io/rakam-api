package org.rakam.realtime;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/02/15 03:20.
 */
public class RealTimeReport {
    @NotNull public final String project;
    public final String name;
    public final AggregationType aggregation;
    public final List<String> collections;

    public final String filter;

    public final String measure;
    public final String dimension;

    @JsonCreator
    public RealTimeReport(@JsonProperty("project") String project,
                          @JsonProperty("name") String name,
                          @JsonProperty("collection") List<String> collections,
                          @JsonProperty("aggregation") AggregationType aggregation,
                          @JsonProperty("filter") String filter,
                          @JsonProperty("measure") String measure,
                          @JsonProperty("dimension") String dimension) {
        this.project = project;
        this.name = name;
        this.collections = collections;
        this.filter = filter;
        this.aggregation = aggregation;
        this.measure = measure;
        this.dimension = dimension;
    }

    public static class RealTimeQueryField {
        public final String field;
        public final String expression;

        @JsonCreator
        public RealTimeQueryField(@JsonProperty("field") String field,
                                  @JsonProperty("expression") String expression) {
            this.field = field;
            this.expression = expression;
        }
    }
}
