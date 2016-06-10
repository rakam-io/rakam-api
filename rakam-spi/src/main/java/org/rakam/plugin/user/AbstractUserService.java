package org.rakam.plugin.user;

import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.rakam.collection.SchemaField;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryResult;
import org.rakam.server.http.annotations.ApiParam;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;


public abstract class AbstractUserService {
    private final UserStorage storage;

    public AbstractUserService(UserStorage storage) {
        this.storage = storage;
    }

    public Object create(String project, Object id, Map<String, Object> properties) {
        return storage.create(project, id, properties);
    }

    public List<Object> batchCreate(String project, List<User> users) {
        return storage.batchCreate(project, users);
    }

    public void createProject(String project, boolean userIdIsNumeric) {
        storage.createProjectIfNotExists(project, userIdIsNumeric);
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

    public CompletableFuture<User> getUser(String project, Object user) {
        return storage.getUser(project, user);
    }

    public void setUserProperties(String project, Object user, Map<String, Object> properties) {
        storage.setUserProperty(project, user, properties);
    }

    public void setUserPropertiesOnce(String project, Object user, Map<String, Object> properties) {
        storage.setUserPropertyOnce(project, user, properties);
    }

    public abstract CompletableFuture<List<CollectionEvent>> getEvents(String project, String user, int limit, Instant beforeThisTime);

    public void incrementProperty(String project, Object user, String property, double value) {
        storage.incrementProperty(project, user, property, value);
    }

    public void unsetProperties(String project, Object user, List<String> properties) {
        storage.unsetProperties(project, user, properties);
    }

    public abstract void merge(String project, String user, String anonymousId, Instant createdAt, Instant mergedAt);

    public abstract QueryExecution precalculate(String project, PreCalculateQuery query);

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
