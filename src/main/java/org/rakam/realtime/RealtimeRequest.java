package org.rakam.realtime;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/02/15 03:20.
 */
public class RealTimeRequest {
    public final AggregationType aggregation;
    @NotNull public final String project;
    public final String collection;

    public final String filter;

    public final RealTimeQueryField measure;
    public final RealTimeQueryField dimension;

    @JsonCreator
    public RealTimeRequest(@JsonProperty("project") String project,
                           @JsonProperty("collection") String collection,
                           @JsonProperty("aggregation") AggregationType aggregation,
                           @JsonProperty("filter") String filter,
                           @JsonProperty("measure") RealTimeQueryField measure,
                           @JsonProperty("dimension") RealTimeQueryField dimension) {
        this.project = project;
        this.collection = collection;
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
