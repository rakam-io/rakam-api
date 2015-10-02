package org.rakam.report;


import java.util.concurrent.CompletableFuture;


public interface QueryExecution {
    QueryStats currentStats();
    boolean isFinished();
    CompletableFuture<QueryResult> getResult();
    String getQuery();
    void kill();
}
