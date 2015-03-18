package org.rakam.report;

import com.facebook.presto.jdbc.internal.client.StatementClient;
import com.facebook.presto.jdbc.internal.client.StatementStats;
import com.facebook.presto.jdbc.internal.guava.collect.Lists;
import com.facebook.presto.jdbc.internal.guava.util.concurrent.ThreadFactoryBuilder;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/03/15 21:57.
 */
public class PrestoQuery implements QueryExecution {
    // doesn't seem to be a good way but presto client uses a synchronous http client
    // so it blocks the thread when executing queries
    private static final ExecutorService QUERY_EXECUTOR = new ThreadPoolExecutor(0, 50, 120L, TimeUnit.SECONDS,
            new SynchronousQueue<>(), new ThreadFactoryBuilder()
            .setNameFormat("presto-query-executor")
            .setUncaughtExceptionHandler((t, e) -> e.printStackTrace()).build());
    private final List<List<Object>> data = Lists.newArrayList();
    private final CompletableFuture<QueryResult> result = new CompletableFuture<>();

    private final StatementClient client;

    public PrestoQuery(StatementClient client) {
        this.client = client;

        QUERY_EXECUTOR.execute(new Runnable() {
            @Override
            public void run() {
                client.advance();
                if (client.isFailed()) {
                    com.facebook.presto.jdbc.internal.client.QueryError error = client.current().getError();
                    QueryError queryError = new QueryError(error.getFailureInfo().getMessage(), error.getSqlState(), error.getErrorCode());
                    result.complete(new QueryResult(null, null, queryError));
                } else if (!client.isValid()) {
                    Optional.ofNullable(client.finalResults().getData())
                            .ifPresent((newResults) -> newResults.forEach(data::add));

                    List<SchemaField> columns = Lists.newArrayList();
                    List<com.facebook.presto.jdbc.internal.client.Column> internalColumns = client.finalResults().getColumns();
                    for (int i = 0; i < internalColumns.size(); i++) {
                        com.facebook.presto.jdbc.internal.client.Column c = internalColumns.get(i);
                        columns.add(new SchemaField(c.getName(), FieldType.valueOf(c.getType()), true));
                    }
                    result.complete(new QueryResult(columns, data, null));
                } else {
                    Optional.ofNullable(client.current().getData())
                            .ifPresent((newResults) -> newResults.forEach(data::add));
                    run();
                }
            }
        });
    }

    @Override
    public QueryStats currentStats() {
        StatementStats stats = client.current().getStats();
        int totalSplits = stats.getTotalSplits();
        int percentage = totalSplits == 0 ? 0 : stats.getCompletedSplits() * 100 / totalSplits;
        return new QueryStats(percentage,
                stats.getState(),
                stats.getNodes(),
                stats.getProcessedRows(),
                stats.getProcessedBytes(),
                stats.getUserTimeMillis(),
                stats.getCpuTimeMillis(),
                stats.getWallTimeMillis());
    }

    @Override
    public boolean isFinished() {
        return !client.isValid();
    }

    @Override
    public CompletableFuture<QueryResult> getResult() {
        return result;
    }

    public String getQuery() {
        return client.getQuery();
    }


}
