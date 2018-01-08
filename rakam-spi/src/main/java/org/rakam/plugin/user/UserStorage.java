package org.rakam.plugin.user;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.RequestContext;
import org.rakam.collection.SchemaField;
import org.rakam.report.QueryResult;
import org.rakam.report.realtime.AggregationType;
import org.rakam.util.RakamException;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;


public interface UserStorage {
    String PRIMARY_KEY = "id";

    Object create(String project, Object id, ObjectNode properties);

    List<Object> batchCreate(RequestContext context, List<User> users);

    default CompletableFuture<Void> batch(String project, List<? extends ISingleUserBatchOperation> operations) {
        for (ISingleUserBatchOperation operation : operations) {
            if (operation.getSetPropertiesOnce() != null) {
                setUserProperties(project, operation.getUser(), operation.getSetProperties());
            }
            if (operation.getSetPropertiesOnce() != null) {
                setUserPropertiesOnce(project, operation.getUser(), operation.getSetPropertiesOnce());
            }
            if (operation.getUnsetProperties() != null) {
                unsetProperties(project, operation.getUser(), operation.getUnsetProperties());
            }
            if (operation.getIncrementProperties() != null) {
                for (Map.Entry<String, Double> entry : operation.getIncrementProperties().entrySet()) {
                    incrementProperty(project, operation.getUser(), entry.getKey(), entry.getValue());
                }
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    CompletableFuture<QueryResult> searchUsers(RequestContext context, List<String> columns, Expression filterExpression, List<EventFilter> eventFilter, Sorting sortColumn, long limit, String offset);

    void createSegment(RequestContext context, String name, String tableName, Expression filterExpression, List<EventFilter> eventFilter, Duration interval);

    List<SchemaField> getMetadata(RequestContext context);

    CompletableFuture<User> getUser(RequestContext context, Object userId);

    void setUserProperties(String project, Object user, ObjectNode properties);

    void setUserPropertiesOnce(String project, Object user, ObjectNode properties);

    default void createProjectIfNotExists(String project, boolean isNumeric) {

    }

    void incrementProperty(String project, Object user, String property, double value);

    void dropProjectIfExists(String project);

    void unsetProperties(String project, Object user, List<String> properties);

    default void applyOperations(String project, List<? extends ISingleUserBatchOperation> req) {
        for (ISingleUserBatchOperation data : req) {
            if (data.getSetProperties() != null) {
                setUserProperties(project, data.getUser(), data.getSetPropertiesOnce());
            }
            if (data.getSetProperties() != null) {
                setUserPropertiesOnce(project, data.getUser(), data.getSetPropertiesOnce());
            }
            if (data.getUnsetProperties() != null) {
                unsetProperties(project, data.getUser(), data.getUnsetProperties());
            }
            if (data.getIncrementProperties() != null) {
                for (Map.Entry<String, Double> entry : data.getIncrementProperties().entrySet()) {
                    incrementProperty(project, data.getUser(), entry.getKey(), entry.getValue());
                }
            }
        }
    }

    enum Ordering {
        asc, desc
    }

    class Sorting {
        public final String column;
        public final Ordering order;

        @JsonCreator
        public Sorting(@JsonProperty("column") String column,
                       @JsonProperty("order") Ordering order) {
            this.column = column;
            this.order = order;
        }
    }

    class EventFilterAggregation {
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

    class Timeframe {
        public final Instant start;
        public final Instant end;

        @JsonCreator
        public Timeframe(@JsonProperty("start") Instant start, @JsonProperty("end") Instant end) {
            this.start = start;
            this.end = end;
        }
    }

    class EventFilter {
        private static final SqlParser SQL_PARSER = new SqlParser();

        public final String collection;
        public final String filterExpression;
        public final Timeframe timeframe;
        public final EventFilterAggregation aggregation;

        @JsonCreator
        public EventFilter(@JsonProperty("collection") String collection,
                           @JsonProperty("filter") String filterExpression,
                           @JsonProperty("timeframe") Timeframe timeframe,
                           @JsonProperty("aggregation") EventFilterAggregation aggregation) {
            this.collection = collection;
            this.filterExpression = filterExpression;
            this.timeframe = timeframe;
            this.aggregation = aggregation;
        }

        @JsonIgnore
        public synchronized Expression getExpression() {
            try {
                return filterExpression != null ? SQL_PARSER.createExpression(filterExpression) : null;
            } catch (Exception e) {
                throw new RakamException(format("filter expression '%s' couldn't parsed", filterExpression), HttpResponseStatus.BAD_REQUEST);
            }
        }

    }

}
