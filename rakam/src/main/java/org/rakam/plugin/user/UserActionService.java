package org.rakam.plugin.user;

import org.rakam.report.QueryResult;
import org.rakam.server.http.HttpService;

import java.util.concurrent.CompletableFuture;

public abstract class UserActionService<T> extends HttpService {

    public abstract CompletableFuture<Long> batch(CompletableFuture<QueryResult> queryResult, T config);
    public abstract String getName();
    public abstract CompletableFuture<Boolean> send(User user, T config);
}
