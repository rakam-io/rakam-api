package org.rakam.plugin.user;

import org.rakam.report.QueryResult;
import org.rakam.server.http.HttpService;

import java.util.concurrent.CompletableFuture;

public abstract class UserActionService<T> extends HttpService {

    public abstract CompletableFuture<Long> batch(String project, CompletableFuture<QueryResult> queryResult, T config);

    public abstract String getName();

    public abstract boolean send(String project, User user, T config);
}
