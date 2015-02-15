package org.rakam.realtime;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/02/15 03:20.
 */
public class RealtimeRequest {
    public final AggregationType aggregation;
    @NotNull public final String project;
    public final String collection;

    public final String filter;

    public final RealtimeQueryField measure;
    public final RealtimeQueryField dimension;

    @JsonCreator
    public RealtimeRequest(@JsonProperty("project") String project,
                           @JsonProperty("collection") String collection,
                           @JsonProperty("aggregation") AggregationType aggregation,
                           @JsonProperty("filter") String filter,
                           @JsonProperty("measure") RealtimeQueryField measure,
                           @JsonProperty("dimension") RealtimeQueryField dimension) {
        this.project = project;
        this.collection = collection;
        this.filter = filter;
        this.aggregation = aggregation;
        this.measure = measure;
        this.dimension = dimension;
    }

    public static class RealtimeQueryField {
        public final String field;
        public final String expression;

        @JsonCreator
        public RealtimeQueryField(@JsonProperty("field") String field,
                                  @JsonProperty("expression") String expression) {
            this.field = field;
            this.expression = expression;

        }
    }
}
