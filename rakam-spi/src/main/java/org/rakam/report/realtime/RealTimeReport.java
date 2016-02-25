package org.rakam.report.realtime;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.rakam.plugin.ProjectItem;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.util.ValidationUtil;

import javax.validation.constraints.NotNull;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;


public class RealTimeReport implements ProjectItem {
    @NotNull public final String project;
    @NotNull public final String name;
    @NotNull public final String table_name;
    @NotNull public final AggregationType aggregation;
    public final List<String> collections;
    public final String filter;
    public final String measure;
    public final List<String> dimensions;

    @JsonCreator
    public RealTimeReport(@ApiParam(name = "project") String project,
                          @ApiParam(name ="name") String name,
                          @ApiParam(name ="aggregation") AggregationType aggregation,
                          @ApiParam(name ="table_name") String tableName,
                          @ApiParam(name ="collections") List<String> collections,
                          @ApiParam(name ="filter", required = false) String filter,
                          @ApiParam(name ="measure", required = false) String measure,
                          @ApiParam(name ="dimensions", required = false) List<String> dimensions) {
        this.project = checkNotNull(project, "project is required");
        this.name = checkNotNull(name, "name is required");
        this.table_name = checkNotNull(tableName, "table_name is required");
        this.collections = checkNotNull(collections, "collections is required");
        this.aggregation = checkNotNull(aggregation, "aggregation is required");
        this.filter = filter;
        this.measure = measure;
        this.dimensions = dimensions;
        ValidationUtil.checkArgument(!collections.isEmpty(), "collections is empty");
    }

    @Override
    public String project() {
        return project;
    }
}
