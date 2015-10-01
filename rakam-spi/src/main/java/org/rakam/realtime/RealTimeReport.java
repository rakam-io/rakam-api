package org.rakam.realtime;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/02/15 03:20.
 */
public class RealTimeReport {
    @NotNull public final String project;
    @NotNull public final String name;
    @NotNull public final String chart;
    @NotNull public final String table_name;
    @NotNull public final AggregationType aggregation;
    public final List<String> collections;

    public final String filter;

    public final String measure;
    public final String dimension;

    @JsonCreator
    public RealTimeReport(@JsonProperty("project") String project,
                          @JsonProperty("name") String name,
                          @JsonProperty("chart") String chart,
                          @JsonProperty("collections") List<String> collections,
                          @JsonProperty("aggregation") AggregationType aggregation,
                          @JsonProperty("table_name") String tableName,
                          @JsonProperty("filter") String filter,
                          @JsonProperty("measure") String measure,
                          @JsonProperty("dimension") String dimension) {
        this.project = checkNotNull(project, "project is required");
        this.chart = checkNotNull(chart, "chart is required");
        this.name = checkNotNull(name, "name is required");
        this.table_name = checkNotNull(tableName, "project is required");
        this.collections = checkNotNull(collections, "collections is required");
        this.filter = filter;
        this.aggregation = checkNotNull(aggregation, "aggregation is required");
        this.measure = measure;
        this.dimension = dimension;
    }
}
