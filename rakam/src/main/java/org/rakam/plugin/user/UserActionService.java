package org.rakam.plugin.user;

import org.rakam.report.QueryResult;

import java.util.concurrent.CompletableFuture;

public interface UserActionService<T> {
    CompletableFuture<Long> batch(CompletableFuture<QueryResult> queryResult, T config);
    CompletableFuture<Boolean> send(User user, T config);
    String getActionName();
    // equals and hashcode must be based on action name
}
