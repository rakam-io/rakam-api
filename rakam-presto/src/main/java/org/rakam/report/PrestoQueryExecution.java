package org.rakam.report;

import com.facebook.presto.jdbc.internal.client.ClientTypeSignature;
import com.facebook.presto.jdbc.internal.client.StatementClient;
import com.facebook.presto.jdbc.internal.client.StatementStats;
import com.facebook.presto.jdbc.internal.guava.collect.Lists;
import com.facebook.presto.jdbc.internal.guava.util.concurrent.ThreadFactoryBuilder;
import com.facebook.presto.jdbc.internal.spi.type.StandardTypes;
import com.google.common.collect.ImmutableMap;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;

import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.rakam.collection.FieldType.*;

public class PrestoQueryExecution implements QueryExecution {
    // doesn't seem to be a good way but presto client uses a synchronous http client
    // so it blocks the thread when executing queries
    private static final ExecutorService QUERY_EXECUTOR = new ThreadPoolExecutor(0, 50, 120L, TimeUnit.SECONDS,
            new SynchronousQueue<>(), new ThreadFactoryBuilder()
            .setNameFormat("presto-query-executor")
            .setUncaughtExceptionHandler((t, e) -> e.printStackTrace()).build());
    private final List<List<Object>> data = Lists.newArrayList();
    private final CompletableFuture<QueryResult> result = new CompletableFuture<>();

    private final StatementClient client;

    public PrestoQueryExecution(StatementClient client) {
        this.client = client;

        QUERY_EXECUTOR.execute(new Runnable() {
            @Override
            public void run() {
                while (client.isValid() && client.advance()) {
                    Optional.ofNullable(client.current().getData())
                            .ifPresent((newResults) -> newResults.forEach(data::add));
                }

                if (client.isFailed()) {
                    com.facebook.presto.jdbc.internal.client.QueryError error = client.finalResults().getError();
                    QueryError queryError = new QueryError(error.getFailureInfo().getMessage(), error.getSqlState(), error.getErrorCode());
                    result.complete(QueryResult.errorResult(queryError));
                } else {
                    Optional.ofNullable(client.finalResults().getData())
                            .ifPresent((newResults) -> newResults.forEach(data::add));

                    List<SchemaField> columns = Lists.newArrayList();
                    List<com.facebook.presto.jdbc.internal.client.Column> internalColumns = client.finalResults().getColumns();
                    for (int i = 0; i < internalColumns.size(); i++) {
                        com.facebook.presto.jdbc.internal.client.Column c = internalColumns.get(i);
                        columns.add(new SchemaField(c.getName(), fromPrestoType(c.getTypeSignature()), true));
                    }
                    result.complete(new QueryResult(columns, data, ImmutableMap.of(QueryResult.EXECUTION_TIME, client.finalResults().getStats().getUserTimeMillis())));
                }
            }
        });
    }

    public static FieldType fromPrestoType(ClientTypeSignature prestoType) {

        switch (prestoType.getRawType()) {
            case StandardTypes.BIGINT:
                return LONG;
            case StandardTypes.BOOLEAN:
                return BOOLEAN;
            case StandardTypes.DATE:
                return DATE;
            case StandardTypes.DOUBLE:
                return DOUBLE;
            case StandardTypes.VARCHAR:
                return STRING;
            case StandardTypes.TIME:
            case StandardTypes.TIME_WITH_TIME_ZONE:
                return TIME;
            case StandardTypes.TIMESTAMP:
            case StandardTypes.TIMESTAMP_WITH_TIME_ZONE:
                return TIMESTAMP;
            default:
                if(prestoType.getRawType().equals(StandardTypes.ARRAY)) {
                    return fromPrestoType(prestoType.getTypeArguments().get(0)).convertToArrayType();
                }
                throw new NoSuchElementException();
        }
    }

    public static String toPrestoType(FieldType type) {

        switch (type) {
            case LONG:
                return StandardTypes.BIGINT;
            case BOOLEAN:
                return StandardTypes.BOOLEAN;
            case DATE:
                return StandardTypes.DATE;
            case DOUBLE:
                return StandardTypes.DOUBLE;
            case STRING:
                return StandardTypes.VARCHAR;
            case TIME:
                return StandardTypes.TIME;
            case TIMESTAMP:
                return StandardTypes.TIMESTAMP;
            default:
                if(type.isArray()) {
                    return StandardTypes.ARRAY+"<"+toPrestoType(type.getArrayElementType())+">";
                }
                throw new NoSuchElementException();
        }
    }

    @Override
    public QueryStats currentStats() {
        StatementStats stats = client.current().getStats();
        int totalSplits = stats.getTotalSplits();
        int percentage = totalSplits == 0 ? 0 : stats.getCompletedSplits() * 100 / totalSplits;
        return new QueryStats(percentage,
                QueryStats.State.valueOf(stats.getState().toUpperCase(Locale.ENGLISH)),
                stats.getNodes(),
                stats.getProcessedRows(),
                stats.getProcessedBytes(),
                stats.getUserTimeMillis(),
                stats.getCpuTimeMillis(),
                stats.getWallTimeMillis());
    }

    @Override
    public boolean isFinished() {
        return result.isDone();
    }

    @Override
    public CompletableFuture<QueryResult> getResult() {
        return result;
    }

    public String getQuery() {
        return client.getQuery();
    }

    @Override
    public void kill() {
        client.close();
    }


}
