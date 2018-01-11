package org.rakam.report;


import java.util.concurrent.CompletableFuture;


public interface QueryExecution {
    static QueryExecution completedQueryExecution(String query, QueryResult result) {
        return new QueryExecution() {
            @Override
            public QueryStats currentStats() {
                return new QueryStats(100, QueryStats.State.FINISHED, null, null, null, null, null, null);
            }

            @Override
            public boolean isFinished() {
                return true;
            }

            @Override
            public CompletableFuture<QueryResult> getResult() {
                return CompletableFuture.completedFuture(result);
            }

            @Override
            public void kill() {
            }
        };
    }

    QueryStats currentStats();

    boolean isFinished();

    CompletableFuture<QueryResult> getResult();

    void kill();
}
