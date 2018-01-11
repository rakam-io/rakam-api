package org.rakam.report.realtime;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.util.RakamException;
import org.rakam.util.ValidationUtil;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;

public class RealTimeReport {
    @NotNull
    public final String name;
    @NotNull
    public final String table_name;
    public final Set<String> collections;
    public final String filter;
    public final List<Measure> measures;
    public final List<String> dimensions;

    @JsonCreator
    public RealTimeReport(@ApiParam("name") String name,
                          @ApiParam("measures") List<Measure> measures,
                          @ApiParam("table_name") String tableName,
                          @ApiParam("collections") Set<String> collections,
                          @ApiParam(value = "filter", required = false) String filter,
                          @ApiParam(value = "dimensions", required = false) List<String> dimensions) {
        this.name = checkNotNull(name, "name is required");
        this.table_name = checkNotNull(tableName, "table_name is required");
        this.collections = checkNotNull(collections, "collections is required");
        this.filter = filter;
        this.measures = checkNotNull(measures, "measures is required");
        this.dimensions = dimensions;
        if (this.measures.isEmpty()) {
            throw new RakamException("There must be at least one measure", BAD_REQUEST);
        }
        for (Measure measure : measures) {
            if (dimensions.stream().anyMatch(dimension -> dimension.equals(measure.column))) {
                throw new RakamException(format("Column %s in dimension cannot be also in measures", measure.column), BAD_REQUEST);
            }
        }
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

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Measure measure = (Measure) o;

            if (!column.equals(measure.column)) {
                return false;
            }
            return aggregation == measure.aggregation;
        }

        @Override
        public int hashCode() {
            int result = column.hashCode();
            result = 31 * result + aggregation.hashCode();
            return result;
        }
    }
}
