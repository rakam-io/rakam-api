package org.rakam.report;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class DelegateQueryExecution implements QueryExecution {

    private final QueryExecution execution;
    private final CompletableFuture<QueryResult> result;

    public DelegateQueryExecution(QueryExecution execution, Function<QueryResult, QueryResult> function) {
        this.execution = execution;
        this.result = execution.getResult().thenApply(function);
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
        return result;
    }

    @Override
    public void kill() {
        execution.kill();
    }
}
