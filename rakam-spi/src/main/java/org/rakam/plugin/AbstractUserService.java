package org.rakam.plugin;

import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.user.User;
import org.rakam.report.QueryResult;

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

    public String create(String project, String id, Map<String, Object> properties) {
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

    public abstract CompletableFuture<List<CollectionEvent>> getEvents(String project, String user, int limit, long offset);

    public void incrementProperty(String project, String user, String property, double value) {
        storage.incrementProperty(project, user, property, value);
    }

    public void unsetProperties(String project, String user, List<String> properties) {
        storage.unsetProperties(project, user, properties);
    }

    public abstract void merge(String project, String user, String anonymousId, Instant createdAt, Instant mergedAt);

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
}
