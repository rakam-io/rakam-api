/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.rakam.analysis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryResult;
import org.rakam.report.eventexplorer.AbstractEventExplorer;
import org.rakam.report.realtime.AggregationType;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.util.RakamException;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.rakam.report.realtime.AggregationType.COUNT;
import static org.rakam.util.ValidationUtil.checkCollection;

public interface EventExplorer {

    CompletableFuture<AbstractEventExplorer.PrecalculatedTable> create(RequestContext context, OLAPTable table);

    QueryExecution analyze(RequestContext context, List<String> collections, Measure measureType, Reference grouping, Reference segment, String filterExpression, LocalDate startDate, LocalDate endDate, ZoneId timezone);

    CompletableFuture<QueryResult> getEventStatistics(RequestContext requestContext, Optional<Set<String>> collections, Optional<String> dimension, LocalDate startDate, LocalDate endDate, ZoneId timezone);

    Map<String, List<String>> getExtraDimensions(String project);

    default String getIntermediateForApproximateUniqueFunction() {
        throw new UnsupportedOperationException();
    }

    default String getFinalForApproximateUniqueFunction() {
        throw new UnsupportedOperationException();
    }

    QueryExecution export(RequestContext context, List<String> collections, Measure measure, Reference grouping, Reference segment, String filterExpression, LocalDate startDate, LocalDate endDate, ZoneId zoneId);

    enum TimestampTransformation {
        HOUR_OF_DAY("Date category", "Hour of day"),
        DAY_OF_MONTH("Date category", "Day of month"),
        WEEK_OF_YEAR("Date category", "Week of year"),
        MONTH_OF_YEAR("Date category", "Month of year"),
        QUARTER_OF_YEAR("Date category", "Quarter of year"),
        DAY_PART("Date category", "Day part"),
        DAY_OF_WEEK("Date category", "Day of week"),

        HOUR("Date period", "Hour"),
        DAY("Date period", "Day"), WEEK("Date period", "Week"),
        MONTH("Date period", "Month"), YEAR("Date period", "Year");

        private final String prettyName;
        private final String category;

        TimestampTransformation(String category, String name) {
            this.prettyName = name;
            this.category = category;
        }

        @JsonCreator
        public static TimestampTransformation fromString(String key) {
            try {
                return key == null ? null : valueOf(key.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new RakamException("Invalid date range value: " + key, BAD_REQUEST);
            }
        }

        public static Optional<TimestampTransformation> fromPrettyName(String name) {
            for (TimestampTransformation transformation : values()) {
                if (transformation.getPrettyName().equals(name)) {
                    return Optional.of(transformation);
                }
            }
            return Optional.empty();
        }

        public String getPrettyName() {
            return prettyName;
        }

        public String getCategory() {
            return category;
        }
    }

    enum ReferenceType {
        COLUMN, REFERENCE;

        @JsonCreator
        public static ReferenceType get(String name) {
            return valueOf(name.toUpperCase());
        }

        @JsonProperty
        public String value() {
            return name();
        }
    }

    class Measure {
        public final String column;
        public final AggregationType aggregation;

        @JsonCreator
        public Measure(@ApiParam(value = "column", required = false) String column,
                       @ApiParam("aggregation") AggregationType aggregation) {
            if (column == null && aggregation != COUNT) {
                throw new IllegalArgumentException("measure column is required if aggregation is not COUNT");
            }
            this.column = column;
            this.aggregation = Objects.requireNonNull(aggregation, "aggregation is null");
        }
    }

    class Reference {
        public final ReferenceType type;
        public final String value;

        @JsonCreator
        public Reference(@ApiParam("type") ReferenceType type,
                         @ApiParam("value") String value) {
            this.type = checkNotNull(type, "type is null");
            this.value = checkNotNull(value, "value is null");
        }
    }

    class OLAPTable {
        public final Set<String> collections;
        public final Set<Dimension> dimensions;
        public final Set<AggregationType> aggregations;
        public final Set<String> measures;
        public final String tableName;

        @JsonCreator
        public OLAPTable(
                @ApiParam("collections") Set<String> collections,
                @ApiParam("dimensions") Set<Dimension> dimensions,
                @ApiParam("aggregations") Set<AggregationType> aggregations,
                @ApiParam("measures") Set<String> measures,
                @ApiParam("tableName") String tableName) {
            checkCollection(tableName);
            this.collections = collections;
            this.dimensions = dimensions;
            this.aggregations = aggregations;
            this.measures = measures;
            this.tableName = tableName;
        }

        public static class Dimension {
            public final String type;
            public final String value;

            @JsonCreator
            Dimension(@ApiParam("type") String type, @ApiParam("value") String value) {
                this.type = type;
                this.value = value;
            }
        }
    }
}
