package org.rakam.report;


import java.util.concurrent.CompletableFuture;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/03/15 21:50.
 */
public interface QueryExecution {
    QueryStats currentStats();
    boolean isFinished();
    CompletableFuture<QueryResult> getResult();
    String getQuery();
}
