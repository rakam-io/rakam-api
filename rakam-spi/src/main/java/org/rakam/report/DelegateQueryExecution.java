package org.rakam.report;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class DelegateQueryExecution implements QueryExecution {

    private final QueryExecution execution;
    private final Function<QueryResult, QueryResult> function;

    public DelegateQueryExecution(QueryExecution execution, Function<QueryResult, QueryResult> function) {
        this.execution = execution;
        this.function = function;
    }

    @Override
    public QueryStats currentStats() {
        return execution.currentStats();
    }

    @Override
    public boolean isFinished() {
        return execution.isFinished();
    }

    @Override
    public CompletableFuture<QueryResult> getResult() {
        return execution.getResult().thenApply(function);
    }

    @Override
    public String getQuery() {
        return execution.getQuery();
    }

    @Override
    public void kill() {
        execution.kill();
    }
}
