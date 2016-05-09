package org.rakam.report.realtime;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.util.ValidationUtil;

import javax.validation.constraints.NotNull;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;


public class RealTimeReport {
    @NotNull public final String name;
    @NotNull public final String table_name;
    public final List<String> collections;
    public final String filter;
    public final List<Measure> measures;
    public final List<String> dimensions;

    @JsonCreator
    public RealTimeReport(@ApiParam("name") String name,
                          @ApiParam("measures") List<Measure> measures,
                          @ApiParam("table_name") String tableName,
                          @ApiParam("collections") List<String> collections,
                          @ApiParam(value = "filter", required = false) String filter,
                          @ApiParam(value = "dimensions", required = false) List<String> dimensions) {
        this.name = checkNotNull(name, "name is required");
        this.table_name = checkNotNull(tableName, "table_name is required");
        this.collections = checkNotNull(collections, "collections is required");
        this.filter = filter;
        this.measures = checkNotNull(measures, "measures is required");;
        this.dimensions = dimensions;
        ValidationUtil.checkArgument(!collections.isEmpty(), "collections is empty");
    }

    public static class Measure {
        public final String column;
        public final AggregationType aggregation;

        @JsonCreator
        public Measure(@ApiParam("column") String column, @ApiParam("aggregation") AggregationType aggregation) {
            this.column = column;
            this.aggregation = aggregation;
        }
    }
}
