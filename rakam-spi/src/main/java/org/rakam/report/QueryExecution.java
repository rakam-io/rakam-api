package org.rakam.report;


import java.util.concurrent.CompletableFuture;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/03/15 21:50.
 */
public interface QueryExecution {
    public QueryStats currentStats();
    public boolean isFinished();
    public CompletableFuture<QueryResult> getResult();
    public String getQuery();
}
