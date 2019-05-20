package org.rakam.plugin.user;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.rakam.analysis.RequestContext;
import org.rakam.collection.SchemaField;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;


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

}
