package org.rakam.report;


import java.util.concurrent.CompletableFuture;


public interface QueryExecution {
    QueryStats currentStats();
    boolean isFinished();
    CompletableFuture<QueryResult> getResult();
    String getQuery();
    void kill();

    static QueryExecution completedQueryExecution(QueryResult result) {
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
            public String getQuery() {
                return null;
            }

            @Override
            public void kill() {
            }
        };
    }
}
