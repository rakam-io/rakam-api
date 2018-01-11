package org.rakam.plugin.user;

import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import org.rakam.analysis.RequestContext;
import org.rakam.collection.SchemaField;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryResult;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.util.RakamException;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

public abstract class AbstractUserService {
    private final UserStorage storage;

    public AbstractUserService(UserStorage storage) {
        this.storage = storage;
    }

    public Object create(String project, Object id, ObjectNode properties) {
        return storage.create(project, id, properties);
    }

    public List<Object> batchCreate(RequestContext context, List<User> users) {
        return storage.batchCreate(context, users);
    }

    @VisibleForTesting
    public void dropProject(String project) {
        storage.dropProjectIfExists(project);
    }

    public void createProject(String project, boolean userIdIsNumeric) {
        storage.createProjectIfNotExists(project, userIdIsNumeric);
    }

    public List<SchemaField> getMetadata(RequestContext context) {
        return storage.getMetadata(context);
    }

    public CompletableFuture<QueryResult> searchUsers(RequestContext context, List<String> columns, Expression filterExpression, List<UserStorage.EventFilter> eventFilter, UserStorage.Sorting sorting, int limit, String offset) {
        return storage.searchUsers(context, columns, filterExpression, eventFilter, sorting, limit, offset);
    }

    public void createSegment(RequestContext context, String name, String tableName, Expression filterExpression, List<UserStorage.EventFilter> eventFilter, Duration interval)
            throws RakamException {
        if (filterExpression == null && (eventFilter == null || eventFilter.isEmpty())) {
            throw new RakamException("At least one filter is required.", BAD_REQUEST);
        }
        storage.createSegment(context, name, tableName, filterExpression, eventFilter, interval);
    }

    public CompletableFuture<User> getUser(RequestContext context, Object user) {
        return storage.getUser(context, user);
    }

    public void setUserProperties(String project, Object user, ObjectNode properties) {
        storage.setUserProperties(project, user, properties);
    }

    public void setUserPropertiesOnce(String project, Object user, ObjectNode properties) {
        storage.setUserPropertiesOnce(project, user, properties);
    }

    public abstract CompletableFuture<List<CollectionEvent>> getEvents(RequestContext context, String user, Optional<List<String>> properties, int limit, Instant beforeThisTime);

    public void incrementProperty(String project, Object user, String property, double value) {
        storage.incrementProperty(project, user, property, value);
    }

    public void unsetProperties(String project, Object user, List<String> properties) {
        storage.unsetProperties(project, user, properties);
    }

    public abstract void merge(String project, Object user, Object anonymousId, Instant createdAt, Instant mergedAt);

    public abstract QueryExecution preCalculate(String project, PreCalculateQuery query);

    public CompletableFuture<Void> batch(String project, List<? extends ISingleUserBatchOperation> batchUserOperations) {
        return storage.batch(project, batchUserOperations);
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

    public static class SingleUserBatchOperationRequest {
        public final Object id;
        public final User.UserContext api;
        public final List<SingleUserBatchOperations> data;

        @JsonCreator
        public SingleUserBatchOperationRequest(
                @ApiParam("id") Object id,
                @ApiParam("api") User.UserContext api,
                @ApiParam("data") List<SingleUserBatchOperations> data) {
            this.id = id;
            this.api = api;
            this.data = data;
            // non-static inner classes doesn't work with Jackson
            // so we pass the outer variable manually.
            data.forEach(op -> op.setUser(id));
        }

        public static class SingleUserBatchOperations
                implements ISingleUserBatchOperation {
            @JsonProperty("set_properties")
            public final ObjectNode setProperties;
            @JsonProperty("set_properties_once")
            public final ObjectNode setPropertiesOnce;
            @JsonProperty("increment_properties")
            public final Map<String, Double> incrementProperties;
            @JsonProperty("unset_properties")
            public final List<String> unsetProperties;
            @JsonProperty("time")
            public final Long time;
            public Object user;

            @JsonCreator
            public SingleUserBatchOperations(
                    @ApiParam(value = "set_properties", required = false) ObjectNode setProperties,
                    @ApiParam(value = "set_properties_once", required = false) ObjectNode setPropertiesOnce,
                    @ApiParam(value = "increment_properties", required = false) Map<String, Double> incrementProperties,
                    @ApiParam(value = "unset_properties", required = false) List<String> unsetProperties,
                    @ApiParam("time") Long time) {
                this.setProperties = setProperties;
                this.setPropertiesOnce = setPropertiesOnce;
                this.incrementProperties = incrementProperties;
                this.unsetProperties = unsetProperties;
                this.time = time;
            }

            @Override
            public ObjectNode getSetProperties() {
                return setProperties;
            }

            @Override
            public ObjectNode getSetPropertiesOnce() {
                return setPropertiesOnce;
            }

            @Override
            public Map<String, Double> getIncrementProperties() {
                return incrementProperties;
            }

            @Override
            public List<String> getUnsetProperties() {
                return unsetProperties;
            }

            @Override
            public Long getTime() {
                return time;
            }

            @Override
            public Object getUser() {
                return user;
            }

            private void setUser(Object user) {
                this.user = user;
            }
        }
    }

    public static class BatchUserOperationRequest {
        public final User.UserContext api;
        public final List<BatchUserOperations> data;

        @JsonCreator
        public BatchUserOperationRequest(
                @ApiParam("api") User.UserContext api,
                @ApiParam("data") List<BatchUserOperations> data) {
            this.api = api;
            this.data = data;
        }

        public static class BatchUserOperations implements ISingleUserBatchOperation {
            @JsonProperty(value = "set_properties")
            private final ObjectNode setProperties;
            @JsonProperty(value = "set_properties_once")
            private final ObjectNode setPropertiesOnce;
            @JsonProperty(value = "increment_properties")
            private final Map<String, Double> incrementProperties;
            @JsonProperty(value = "unset_properties")
            private final List<String> unsetProperties;
            @JsonProperty("id")
            public Object id;
            @JsonProperty(value = "time")
            private Long time;

            @JsonCreator
            public BatchUserOperations(
                    @ApiParam("id") Object id,
                    @ApiParam(value = "set_properties", required = false) ObjectNode setProperties,
                    @ApiParam(value = "set_properties_once", required = false) ObjectNode setPropertiesOnce,
                    @ApiParam(value = "increment_properties", required = false) Map<String, Double> incrementProperties,
                    @ApiParam(value = "unset_properties", required = false) List<String> unsetProperties,
                    @ApiParam(value = "time", required = false) Long time) {
                this.id = id;
                this.setProperties = setProperties;
                this.setPropertiesOnce = setPropertiesOnce;
                this.incrementProperties = incrementProperties;
                this.unsetProperties = unsetProperties;
                this.time = time;
            }

            @JsonIgnore
            public Object getUser() {
                return id;
            }

            @Override
            public ObjectNode getSetProperties() {
                return setProperties;
            }

            @Override
            public ObjectNode getSetPropertiesOnce() {
                return setPropertiesOnce;
            }

            @Override
            public Map<String, Double> getIncrementProperties() {
                return incrementProperties;
            }

            @Override
            public List<String> getUnsetProperties() {
                return unsetProperties;
            }

            @Override
            public Long getTime() {
                return time;
            }
        }
    }

}
