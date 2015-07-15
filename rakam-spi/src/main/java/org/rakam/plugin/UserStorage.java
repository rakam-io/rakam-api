package org.rakam.plugin;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.rakam.plugin.user.User;
import org.rakam.report.QueryResult;
import org.rakam.util.RakamException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/03/15 21:32.
 */
public interface UserStorage {
    public Object create(String project, Map<String, Object> properties);
    public CompletableFuture<QueryResult> filter(String project, Expression filterExpression, List<EventFilter> eventFilter, Sorting sortColumn, long limit, long offset);
    public List<Column> getMetadata(String project);
    public CompletableFuture<User> getUser(String project, String userId);
    void setUserProperty(String project, String user, String property, Object value);
    void createProject(String project);

    public static class Sorting {
        public final String column;
        public final Ordering order;

        @JsonCreator
        public Sorting(@JsonProperty("column") String column,
                       @JsonProperty("order") Ordering order) {
            this.column = column;
            this.order = order;
        }
    }

    public static enum Ordering {
        asc, desc
    }

    public static class EventFilterAggregation {
        public final AggregationType type;
        public final String field;
        public final Long minimum;
        public final Long maximum;

        @JsonCreator
        public EventFilterAggregation(@JsonProperty("aggregation") AggregationType type,
                                      @JsonProperty("field") String field,
                                      @JsonProperty("minimum") Long minimum,
                                      @JsonProperty("maximum") Long maximum) {
            this.type = type;
            this.field = field;
            this.minimum = minimum;
            this.maximum = maximum;
        }
    }

    public static class EventFilter {
        public static final SqlParser SQL_PARSER = new SqlParser();

        public final String collection;
        public final Expression filterExpression;
        public final EventFilterAggregation aggregation;

        @JsonCreator
        public EventFilter(@JsonProperty("collection") String collection,
                           @JsonProperty("filter") String filterExpression,
                           @JsonProperty("aggregation") EventFilterAggregation aggregation) {
            this.collection = collection;
            if(filterExpression != null) {
                try {
                    synchronized (SQL_PARSER) {
                        this.filterExpression = SQL_PARSER.createExpression(filterExpression);
                    }
                } catch (Exception e) {
                    throw new RakamException(format("filter expression '%s' couldn't parsed", filterExpression), 400);
                }
            }else {
                this.filterExpression = null;
            }
            this.aggregation = aggregation;
        }

    }

    public static enum AggregationType {
        COUNT,
        SUM,
        MINIMUM,
        MAXIMUM,
        APPROXIMATE_UNIQUE,
        VARIANCE,
        POPULATION_VARIANCE,
        STANDARD_DEVIATION,
        AVERAGE;

        @JsonCreator
        public static AggregationType get(String name) {
            return valueOf(name.toUpperCase());
        }

        @JsonProperty
        public String value() {
            return name();
        }
    }
}
