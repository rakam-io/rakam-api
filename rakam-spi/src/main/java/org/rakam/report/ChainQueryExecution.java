package org.rakam.report;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class ChainQueryExecution implements QueryExecution {
    private final List<QueryExecution> executions;
    private final String query;
    private final Optional<CompletableFuture<QueryExecution>> chainedQuery;

    public ChainQueryExecution(List<QueryExecution> executions, String query, Supplier<QueryExecution> chainedQuery) {
        this(executions, query, Optional.of(chainedQuery));
    }

    public ChainQueryExecution(List<QueryExecution> executions, String query) {
        this(executions, query, Optional.empty());
    }

    public ChainQueryExecution(List<QueryExecution> executions, String query, Optional<Supplier<QueryExecution>> chainedQuery) {
        this.executions = executions;
        this.query = query;

        this.chainedQuery = chainedQuery.map(q -> CompletableFuture
                .allOf(executions.stream().map(e -> e.getResult())
                        .toArray(CompletableFuture[]::new)).thenApply(r -> q.get()));
    }

    @Override
    public QueryStats currentStats() {
        QueryStats currentStats = null;
        for (QueryExecution queryExecution : executions) {
            QueryStats queryStats = queryExecution.currentStats();
            if (currentStats == null) {
                currentStats = queryStats;
            } else {
                currentStats = merge(currentStats, queryStats);
            }
        }

        if (chainedQuery.isPresent() && chainedQuery.get().isDone()) {
            currentStats = merge(currentStats, chainedQuery.get().join().currentStats());
        }

        return currentStats;
    }

    private QueryStats merge(QueryStats currentStats, QueryStats stats) {
        return new QueryStats(currentStats.percentage + stats.percentage,
                currentStats.state.equals(stats.state) ? currentStats.state : QueryStats.State.RUNNING,
                Math.max(currentStats.node, stats.node),
                stats.processedRows + currentStats.processedRows,
                stats.processedBytes + currentStats.processedBytes,
                stats.userTime + currentStats.userTime,
                stats.cpuTime + currentStats.cpuTime,
                stats.wallTime + currentStats.wallTime
        );
    }

    @Override
    public boolean isFinished() {
        if (chainedQuery.isPresent() && chainedQuery.get().isDone()) {
            QueryExecution join = chainedQuery.get().join();
            return join == null || join.isFinished();
        } else {
            return false;
        }
    }

    @Override
    public CompletableFuture<QueryResult> getResult() {
        if (!chainedQuery.isPresent()) {
            return CompletableFuture
                    .allOf(executions.stream().map(e -> e.getResult())
                            .toArray(CompletableFuture[]::new)).thenApply(r -> QueryResult.empty());
        }

        CompletableFuture<QueryResult> future = new CompletableFuture<>();
        chainedQuery.get().thenAccept(r -> {
            if (r == null) {
                future.complete(null);
            } else {
                r.getResult().thenAccept(result -> {
                    future.complete(result);
                });
            }
        });
        return future;
    }

    @Override
    public String getQuery() {
        return query;
    }

    @Override
    public void kill() {
        executions.forEach(org.rakam.report.QueryExecution::kill);
        if (chainedQuery.isPresent()) {
            chainedQuery.get().thenAccept(q -> q.kill());
        }
    }
}
