package org.rakam.plugin.user;

import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.report.DelegateQueryExecution;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryResult;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;


public abstract class AbstractUserService {
    private final UserStorage storage;
    private final ContinuousQueryService continuousQueryService;

    public AbstractUserService(ContinuousQueryService continuousQueryService, UserStorage storage) {
        this.continuousQueryService = continuousQueryService;
        this.storage = storage;
    }

    public String create(String project, String id, Map<String, Object> properties) {
        if (id == null) {
            id = UUID.randomUUID().toString();
        }
        return storage.create(project, id, properties);
    }

    public List<String> batchCreate(String project, List<User> users) {
        return storage.batchCreate(project, users);
    }

    public void createProject(String project) {
        storage.createProject(project);
    }

    public List<SchemaField> getMetadata(String project) {
        return storage.getMetadata(project);
    }

    public CompletableFuture<QueryResult> filter(String project, List<String> columns, Expression filterExpression, List<UserStorage.EventFilter> eventFilter, UserStorage.Sorting sorting, int limit, String offset) {
        return storage.filter(project, columns, filterExpression, eventFilter, sorting, limit, offset);
    }

    public void createSegment(String project, String name, String tableName, Expression filterExpression, List<UserStorage.EventFilter> eventFilter, Duration interval) {
        storage.createSegment(project, name, tableName, filterExpression, eventFilter, interval);
    }

    public CompletableFuture<User> getUser(String project, String user) {
        return storage.getUser(project, user);
    }

    public void setUserProperties(String project, String user, Map<String, Object> properties) {
        storage.setUserProperty(project, user, properties);
    }

    public void setUserPropertiesOnce(String project, String user, Map<String, Object> properties) {
        storage.setUserPropertyOnce(project, user, properties);
    }

    public abstract CompletableFuture<List<CollectionEvent>> getEvents(String project, String user, int limit, Instant beforeThisTime);

    public void incrementProperty(String project, String user, String property, double value) {
        storage.incrementProperty(project, user, property, value);
    }

    public void unsetProperties(String project, String user, List<String> properties) {
        storage.unsetProperties(project, user, properties);
    }

    public abstract void merge(String project, String user, String anonymousId, Instant createdAt, Instant mergedAt);

    public QueryExecution precalculate(String project, PreCalculateQuery query) {
        String tableName = "_users_daily" +
                Optional.ofNullable(query.collection).map(value -> "_" + value).orElse("") +
                Optional.ofNullable(query.dimension).map(value -> "_by_" + value).orElse("");

        String name = "Daily users who did " +
                Optional.ofNullable(query.collection).map(value -> " event " + value).orElse(" at least one event") +
                Optional.ofNullable(query.dimension).map(value -> " grouped by " + value).orElse("");

        String table, dateColumn;
        if (query.collection == null) {
            table = String.format("SELECT cast(_time as date) as date, %s _user FROM _all",
                    Optional.ofNullable(query.dimension).map(v -> v + ",").orElse(""));
            dateColumn = "date";
        } else {
            table = "\"" + query.collection + "\"";
            dateColumn = "cast(_time as date)";
        }

        String sqlQuery = String.format("SELECT %s as date, %s set(_user) _user_set FROM (%s) GROUP BY 1 %s",
                dateColumn,
                Optional.ofNullable(query.dimension).map(v -> v + " as dimension,").orElse(""), table,
                Optional.ofNullable(query.dimension).map(v -> ", 2").orElse(""));

        return new DelegateQueryExecution(continuousQueryService.create(project, new ContinuousQuery(name, tableName, sqlQuery,
                ImmutableList.of("date"), ImmutableMap.of()), true), result -> {
            if (result.isFailed()) {
                throw new RakamException("Failed to create continuous query: " + JsonHelper.encode(result.getError()), INTERNAL_SERVER_ERROR);
            }
            result.setProperty("preCalculated", new PreCalculatedTable(name, tableName));
            return result;
        });
    }

    public static class CollectionEvent {
        public final String collection;
        public final Map<String, Object> properties;

        @JsonCreator
        public CollectionEvent(@JsonProperty("collection") String collection,
                               @JsonProperty("properties") Map<String, Object> properties) {
            this.properties = properties;
            this.collection = collection;
        }
    }

    public static class PreCalculateQuery {
        public final String collection;
        public final String dimension;

        public PreCalculateQuery(@ApiParam(value = "collection", required = false) String collection,
                                 @ApiParam(value = "dimension", required = false) String dimension) {
            this.collection = collection;
            this.dimension = dimension;
        }
    }

    public static class PreCalculatedTable {
        public final String name;
        public final String tableName;

        public PreCalculatedTable(String name, String tableName) {
            this.name = name;
            this.tableName = tableName;
        }
    }
}
