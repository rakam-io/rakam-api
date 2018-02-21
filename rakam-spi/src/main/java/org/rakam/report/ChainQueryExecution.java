package org.rakam.report;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Optional.ofNullable;
import static org.rakam.report.QueryStats.State.RUNNING;

public class ChainQueryExecution
        implements QueryExecution {
    private final List<QueryExecution> executions;
    private final CompletableFuture<QueryExecution> chainedQuery;
    private final CompletableFuture<QueryResult> result;

    public ChainQueryExecution(List<QueryExecution> executions, Function<List<QueryResult>, QueryExecution> chainedQuery) {
        this.executions = executions;

        if (chainedQuery != null) {
            CompletableFuture<Void> afterAll = CompletableFuture
                    .allOf(executions.stream().map(e -> e.getResult())
                            .toArray(CompletableFuture[]::new));
            this.chainedQuery = afterAll.thenApply(r -> {
                List<QueryResult> collect = executions.stream().map(e -> e.getResult().join())
                        .collect(Collectors.toList());
                return chainedQuery.apply(collect);
            });

            this.result = new CompletableFuture<>();
            this.chainedQuery.thenAccept(r -> {
                if (r == null) {
                    this.result.complete(null);
                } else {
                    r.getResult().thenAccept(result -> {
                        this.result.complete(result);
                    });
                }
            });
        } else {
            this.chainedQuery = null;
            this.result = CompletableFuture
                    .allOf(executions.stream().map(e -> e.getResult())
                            .toArray(CompletableFuture[]::new)).thenApply(r -> QueryResult.empty());
        }
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

        if (chainedQuery != null && chainedQuery.isDone()) {
            currentStats = merge(currentStats, chainedQuery.join().currentStats());
        }

        return currentStats;
    }

    private QueryStats merge(QueryStats currentStats, QueryStats stats) {
        return new QueryStats(
                ofNullable(currentStats.percentage).orElse(0) + ofNullable(stats.percentage).orElse(0),
                Objects.equals(currentStats.state, stats.state) ? currentStats.state : RUNNING,
                (currentStats.node != null && stats.node != null) ?
                        Math.max(currentStats.node, stats.node) :
                        currentStats.node != null ? currentStats.node : stats.node,
                ofNullable(stats.processedRows).orElse(0L) + ofNullable(currentStats.processedRows).orElse(0L),
                ofNullable(stats.processedBytes).orElse(0L) + ofNullable(currentStats.processedBytes).orElse(0L),
                ofNullable(stats.userTime).orElse(0L) + ofNullable(currentStats.userTime).orElse(0L),
                ofNullable(stats.cpuTime).orElse(0L) + ofNullable(currentStats.cpuTime).orElse(0L),
                ofNullable(stats.wallTime).orElse(0L) + ofNullable(currentStats.wallTime).orElse(0L)
        );
    }

    @Override
    public boolean isFinished() {
        if (chainedQuery != null && chainedQuery.isDone()) {
            QueryExecution join = chainedQuery.join();
            return join == null || join.isFinished();
        } else {
            return false;
        }
    }

    @Override
    public CompletableFuture<QueryResult> getResult() {
        return this.result;
    }

    @Override
    public void kill() {
        executions.forEach(org.rakam.report.QueryExecution::kill);
        if (chainedQuery != null) {
            chainedQuery.thenAccept(q -> q.kill());
        }
    }
}
