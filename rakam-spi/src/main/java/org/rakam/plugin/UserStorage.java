package org.rakam.plugin;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.user.User;
import org.rakam.realtime.AggregationType;
import org.rakam.report.QueryResult;
import org.rakam.util.RakamException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;


public interface UserStorage {
    String create(String project, Map<String, Object> properties);
    List<String> batchCreate(String project, List<User> users);
    CompletableFuture<QueryResult> filter(String project, Expression filterExpression, List<EventFilter> eventFilter, Sorting sortColumn, long limit, long offset);
    List<SchemaField> getMetadata(String project);
    CompletableFuture<User> getUser(String project, String userId);
    void setUserProperty(String project, String user, Map<String, Object> properties);
    void setUserPropertyOnce(String project, String user, Map<String, Object> properties);
    void createProject(String project);
    void incrementProperty(String project, String user, String property, long value);

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

    enum Ordering {
        asc, desc
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

    class EventFilter {
        private static final SqlParser SQL_PARSER = new SqlParser();

        public final String collection;
        public final String filterExpression;
        public final EventFilterAggregation aggregation;

        @JsonCreator
        public EventFilter(@JsonProperty("collection") String collection,
                           @JsonProperty("filter") String filterExpression,
                           @JsonProperty("aggregation") EventFilterAggregation aggregation) {
            this.collection = collection;
            this.filterExpression = filterExpression;
            this.aggregation = aggregation;
        }

        @JsonIgnore
        public synchronized Expression getExpression() {
            try {
                return filterExpression  != null ? SQL_PARSER.createExpression(filterExpression) : null;
            } catch (Exception e) {
                throw new RakamException(format("filter expression '%s' couldn't parsed", filterExpression), HttpResponseStatus.UNAUTHORIZED);
            }
        }

    }

}
