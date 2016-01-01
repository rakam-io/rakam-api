package org.rakam.report;

import com.facebook.presto.jdbc.internal.client.ClientTypeSignature;
import com.facebook.presto.jdbc.internal.client.QueryResults;
import com.facebook.presto.jdbc.internal.client.StatementClient;
import com.facebook.presto.jdbc.internal.client.StatementStats;
import com.facebook.presto.jdbc.internal.guava.collect.Lists;
import com.facebook.presto.jdbc.internal.guava.util.concurrent.ThreadFactoryBuilder;
import com.facebook.presto.jdbc.internal.spi.type.StandardTypes;
import com.google.common.collect.ImmutableMap;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.time.ZoneOffset.UTC;
import static org.rakam.collection.FieldType.*;

public class PrestoQueryExecution implements QueryExecution {
    // doesn't seem to be a good way but presto client uses a synchronous http client
    // so it blocks the thread when executing queries
    private static final ExecutorService QUERY_EXECUTOR = new ThreadPoolExecutor(0, 50, 120L, TimeUnit.SECONDS,
            new SynchronousQueue<>(), new ThreadFactoryBuilder()
            .setNameFormat("presto-query-executor")
            .setUncaughtExceptionHandler((t, e) -> e.printStackTrace()).build());
    private final List<List<Object>> data = Lists.newArrayList();
    private List<SchemaField> columns;

    private final CompletableFuture<QueryResult> result = new CompletableFuture<>();
    private static final DateTimeFormatter PRESTO_TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-M-d H:m:s.SSS");

    private final StatementClient client;
    private final Instant startTime;

    public PrestoQueryExecution(StatementClient client) {
        this.client = client;
        this.startTime = Instant.now();

        QUERY_EXECUTOR.execute(new Runnable() {
            @Override
            public void run() {
                while (client.isValid() && client.advance()) {
                    transformAndAdd(client.current());
                }

                if (client.isFailed()) {
                    com.facebook.presto.jdbc.internal.client.QueryError error = client.finalResults().getError();
                    QueryError queryError = new QueryError(error.getFailureInfo().getMessage(), error.getSqlState(), error.getErrorCode());
                    result.complete(QueryResult.errorResult(queryError));
                } else {
                    transformAndAdd(client.finalResults());

                    ImmutableMap<String, Object> stats = ImmutableMap.of(
                            QueryResult.EXECUTION_TIME, startTime.until(Instant.now(), ChronoUnit.MILLIS));

                    result.complete(new QueryResult(columns, data, stats));
                }
            }

            private void transformAndAdd(QueryResults result) {
                if(result.getError() != null) {
                    return;
                }

                if(columns == null) {
                    columns = result.getColumns().stream()
                            .map(c -> new SchemaField(c.getName(), fromPrestoType(c.getTypeSignature())))
                            .collect(Collectors.toList());
                }

                if(result.getData() == null) {
                    return;
                }

                for (List<Object> objects : result.getData()) {
                    Object[] row = new Object[columns.size()];

                    for (int i = 0; i < objects.size(); i++) {
                        if(columns.get(i).getType() == TIMESTAMP) {
                            row[i] = LocalDateTime.parse((CharSequence) objects.get(i), PRESTO_TIMESTAMP_FORMAT).toInstant(UTC);
                        } else {
                            row[i] = objects.get(i);
                        }
                    }

                    data.add(Arrays.asList(row));
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
            case StandardTypes.ARRAY:
                return fromPrestoType(prestoType.getTypeArguments().get(0)).convertToArrayType();
            case StandardTypes.MAP:
                return fromPrestoType(prestoType.getTypeArguments().get(0)).convertToMapValueType();
            default:
                throw new UnsupportedOperationException();
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
