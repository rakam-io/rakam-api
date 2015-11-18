package org.rakam.plugin;

import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.user.User;
import org.rakam.report.QueryResult;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;


public abstract class AbstractUserService {
    private final UserStorage storage;

    public AbstractUserService(UserStorage storage) {
        this.storage = storage;
    }

    public String create(String project, Map<String, Object> properties) {
        return storage.create(project, properties);
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

    public CompletableFuture<QueryResult> filter(String project, Expression filterExpression, List<UserStorage.EventFilter> eventFilter, UserStorage.Sorting sorting, int limit, int offset) {
        return storage.filter(project, filterExpression, eventFilter, sorting, limit, offset);
    }

    public CompletableFuture<User> getUser(String project, String user) {
        return storage.getUser(project, user);
    }

    public void setUserProperty(String project, String user, String property, Object value) {
        storage.setUserProperty(project, user, property, value);
    }

    public abstract CompletableFuture<List<CollectionEvent>> getEvents(String project, String user, int limit, long offset);

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
