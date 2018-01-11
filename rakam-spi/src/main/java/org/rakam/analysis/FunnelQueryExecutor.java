package org.rakam.analysis;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.report.QueryExecution;
import org.rakam.util.RakamException;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

import static org.rakam.util.ValidationUtil.checkCollection;

public interface FunnelQueryExecutor {
    QueryExecution query(RequestContext context,
                         List<FunnelStep> steps,
                         Optional<String> dimension,
                         Optional<String> segment,
                         LocalDate startDate,
                         LocalDate endDate,
                         Optional<FunnelWindow> window, ZoneId zoneId,
                         Optional<List<String>> connectors,
                         FunnelType type);

    enum FunnelTimestampSegments {
        HOUR_OF_DAY("Hour of day"),
        DAY_OF_MONTH("Day of month"),
        WEEK_OF_YEAR("Week of year"),
        MONTH_OF_YEAR("Month of year"),
        QUARTER_OF_YEAR("Quarter of year"),
        DAY_PART("Day part"),
        DAY_OF_WEEK("Day of week"),
        HOUR("Hour"),
        DAY("Day"), WEEK("Week"),
        MONTH("Month"),
        YEAR("Year");
        private final String displayName;

        FunnelTimestampSegments(String displayName) {
            this.displayName = displayName;
        }

        public String getDisplayName() {
            return displayName;
        }

        @JsonCreator
        FunnelTimestampSegments get(String name) {
            return valueOf(name.toUpperCase());
        }

        @JsonProperty
        public String value() {
            return name();
        }

    }

    enum FunnelType {
        NORMAL, APPROXIMATE, ORDERED;

        @JsonCreator
        public static FunnelType get(String name) {
            return valueOf(name.toUpperCase());
        }

        @JsonProperty
        public String value() {
            return name();
        }
    }


    enum WindowType {
        DAY, WEEK, MONTH;

        @JsonCreator
        public static WindowType get(String name) {
            return valueOf(name.toUpperCase());
        }

        @JsonProperty
        public String value() {
            return name();
        }
    }

    class FunnelWindow {
        public final int value;
        public final WindowType type;

        @JsonCreator
        public FunnelWindow(@JsonProperty("value") int value,
                            @JsonProperty("type") WindowType type) {
            this.value = value;
            this.type = type;
        }
    }

    class FunnelStep {
        private static SqlParser parser = new SqlParser();

        private final String collection;
        private final Optional<String> filterExpression;

        @JsonCreator
        public FunnelStep(@JsonProperty("collection") String collection,
                          @JsonProperty("filterExpression") Optional<String> filterExpression) {
            checkCollection(collection);
            this.collection = collection;
            this.filterExpression = filterExpression == null ? Optional.empty() : filterExpression;
        }

        public String getCollection() {
            return collection;
        }

        @JsonIgnore
        public synchronized Optional<Expression> getExpression() {
            try {
                return filterExpression.map(value -> parser.createExpression(value));
            } catch (Exception e) {
                throw new RakamException("Unable to parse filter expression: " + filterExpression.get(),
                        HttpResponseStatus.BAD_REQUEST);
            }
        }
    }
}
