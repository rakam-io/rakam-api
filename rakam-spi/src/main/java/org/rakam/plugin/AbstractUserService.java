package org.rakam.plugin;

import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.rakam.plugin.user.User;
import org.rakam.report.QueryResult;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Created by buremba <Burak Emre Kabakcı> on 29/04/15 20:09.
 */
public abstract class AbstractUserService {
    private final UserStorage storage;

    public AbstractUserService(UserStorage storage) {
        this.storage = storage;
    }

    public Object create(String project, Map<String, Object> properties) {
        return storage.create(project, properties);
    }

    public void createProject(String project) {
        storage.createProject(project);
    }

    public List<Column> getMetadata(String project) {
        return storage.getMetadata(project);
    }

    public CompletableFuture<QueryResult> filter(String project, Expression filterExpression, List<UserStorage.EventFilter> eventFilter, UserStorage.Sorting sorting, int limit, int offset) {
        return storage.filter(project, filterExpression, eventFilter, sorting, limit, offset);
    }

    public CompletableFuture<User> getUser(String project, String user) {
        return storage.getUser(project, user);
    }

    public void setUserProperty(String project, Object user, String property, Object value) {
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
