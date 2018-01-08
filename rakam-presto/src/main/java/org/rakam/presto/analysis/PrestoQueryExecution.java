package org.rakam.presto.analysis;

import com.facebook.presto.client.*;
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.log.Logger;
import io.netty.handler.codec.http.HttpResponseStatus;
import okhttp3.OkHttpClient;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.report.QueryError;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryResult;
import org.rakam.report.QueryStats;
import org.rakam.util.LogUtil;
import org.rakam.util.RakamException;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.rakam.collection.FieldType.*;
import static org.rakam.report.QueryStats.State.FINISHED;

public class PrestoQueryExecution
        implements QueryExecution {
    public static final DateTimeFormatter PRESTO_TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    public static final DateTimeFormatter PRESTO_TIMESTAMP_WITH_TIMEZONE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS z");
    private final static Logger LOGGER = Logger.get(PrestoQueryExecution.class);
    private static final OkHttpClient HTTP_CLIENT = new OkHttpClient().newBuilder()
            .build();
    private static final ThreadPoolExecutor QUERY_EXECUTOR = new ThreadPoolExecutor(0, 1000,
            60L, TimeUnit.SECONDS,
            new SynchronousQueue<>(), new ThreadFactoryBuilder()
            .setNameFormat("presto-query-executor").build());
    private static final String SERVER_NOT_ACTIVE = "Database server is not active.";
    private final List<List<Object>> data = Lists.newArrayList();
    private final String query;
    private final boolean update;
    private final CompletableFuture<QueryResult> result;
    private final Instant startTime;
    private List<SchemaField> columns;
    private StatementClient client;

    public PrestoQueryExecution(ClientSession session, String query, boolean update) {
        this.startTime = Instant.now();
        this.query = query;
        this.update = update;
        try {
            QUERY_EXECUTOR.execute(new QueryTracker(session));
        } catch (RejectedExecutionException e) {
            throw new RakamException("There are already 1000 running queries. Please calm down.", HttpResponseStatus.TOO_MANY_REQUESTS);
        }

        result = new CompletableFuture<>();
    }

    public static FieldType fromPrestoType(String rawType, Iterator<String> parameter) {
        switch (rawType) {
            case StandardTypes.BIGINT:
                return LONG;
            case StandardTypes.BOOLEAN:
                return BOOLEAN;
            case StandardTypes.DATE:
                return DATE;
            case StandardTypes.DOUBLE:
                return DOUBLE;
            case StandardTypes.VARBINARY:
            case StandardTypes.HYPER_LOG_LOG:
                return BINARY;
            case StandardTypes.VARCHAR:
                return STRING;
            case StandardTypes.INTEGER:
                return INTEGER;
            case StandardTypes.DECIMAL:
                return DECIMAL;
            case StandardTypes.TIME:
            case StandardTypes.TIME_WITH_TIME_ZONE:
                return TIME;
            case StandardTypes.TIMESTAMP:
            case StandardTypes.TIMESTAMP_WITH_TIME_ZONE:
                return TIMESTAMP;
            case StandardTypes.ARRAY:
                return fromPrestoType(parameter.next(), null).convertToArrayType();
            case StandardTypes.MAP:
                Preconditions.checkArgument(parameter.next().equals(StandardTypes.VARCHAR),
                        "The first parameter of MAP must be STRING");
                return fromPrestoType(parameter.next(), null).convertToMapValueType();
            default:
                return BINARY;
        }
    }

    public static boolean isServerInactive(QueryError error) {
        return error.message.startsWith(SERVER_NOT_ACTIVE);
    }

    @Override
    public QueryStats currentStats() {
        if (client == null) {
            return new QueryStats(QueryStats.State.WAITING_FOR_AVAILABLE_THREAD);
        }

        if (client.isFailed()) {
            return new QueryStats(QueryStats.State.FAILED);
        }

        StatementStats stats = client.getStats();

        int totalSplits = stats.getTotalSplits();
        QueryStats.State state = QueryStats.State.valueOf(stats.getState().toUpperCase(Locale.ENGLISH));

        int percentage = state == FINISHED ? 100 : (totalSplits == 0 ? 0 : stats.getCompletedSplits() * 100 / totalSplits);
        return new QueryStats(stats.isScheduled() ? percentage : null,
                state,
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

    @Override
    public void kill() {
        if (!update) {
            client.close();
        }
    }

    private class QueryTracker
            implements Runnable {
        private final ClientSession session;
        private final ZoneId zone;

        public QueryTracker(ClientSession session) {
            this.session = session;
            this.zone = Optional.ofNullable(session.getTimeZone()).map(e -> ZoneId.of(e.getId())).orElse(ZoneOffset.UTC);
        }

        private void waitForQuery() {
            while (client.isValid()) {
                if (Thread.currentThread().isInterrupted()) {
                    client.close();
                    throw new RakamException("Query executor thread was interrupted", INTERNAL_SERVER_ERROR);
                }
                transformAndAdd();

                client.advance();
            }
        }

        @Override
        public void run() {
            try {
                client = new StatementClient(HTTP_CLIENT, session, query);
            } catch (RuntimeException e) {
                String message = SERVER_NOT_ACTIVE + " " + e.getMessage();
                LOGGER.warn(e, message);
                result.complete(QueryResult.errorResult(QueryError.create(message), query));
                return;
            }

            try {
                waitForQuery();

                if (client.isClosed()) {
                    QueryError queryError = QueryError.create("Query aborted by user");
                    result.complete(QueryResult.errorResult(queryError, query));
                } else if (client.isGone()) {
                    QueryError queryError = QueryError.create("Query is gone (server restarted?)");
                    result.complete(QueryResult.errorResult(queryError, query));
                } else if (client.isFailed()) {
                    com.facebook.presto.client.QueryError error = client.finalStatusInfo().getError();
                    com.facebook.presto.client.ErrorLocation errorLocation = error.getErrorLocation();
                    QueryError queryError = new QueryError(
                            Optional.ofNullable(error.getFailureInfo().getMessage())
                                    .orElse(error.getFailureInfo().toException().toString()),
                            error.getSqlState(),
                            error.getErrorCode(),
                            errorLocation != null ? errorLocation.getLineNumber() : null,
                            errorLocation != null ? errorLocation.getColumnNumber() : null);
                    LogUtil.logQueryError(query, queryError, PrestoQueryExecutor.class);
                    result.complete(QueryResult.errorResult(queryError, query));
                } else {
                    transformAndAdd();

                    ImmutableMap<String, Object> stats = ImmutableMap.of(
                            QueryResult.EXECUTION_TIME, startTime.until(Instant.now(), ChronoUnit.MILLIS),
                            QueryResult.QUERY, query);

                    result.complete(new QueryResult(columns, data, stats));
                }
            } catch (Exception e) {
                QueryError queryError = QueryError.create(e.getMessage());
                LogUtil.logQueryError(query, queryError, PrestoQueryExecutor.class);
                result.complete(QueryResult.errorResult(queryError, query));
            }
        }

        private void transformAndAdd() {
            QueryStatusInfo queryStatusInfo = client.isValid() ? client.currentStatusInfo() : client.finalStatusInfo();

            if (queryStatusInfo.getError() != null || queryStatusInfo.getColumns() == null || !client.isValid()) {
                return;
            }

            if (columns == null) {
                columns = queryStatusInfo.getColumns().stream()
                        .map(c -> {
                            List<ClientTypeSignatureParameter> arguments = c.getTypeSignature().getArguments();
                            return new SchemaField(c.getName(), fromPrestoType(c.getTypeSignature().getRawType(),
                                    arguments.stream()
                                            .filter(argument -> argument.getKind() == com.facebook.presto.spi.type.ParameterKind.TYPE)
                                            .map(argument -> argument.getTypeSignature().getRawType()).iterator()));
                        })
                        .collect(Collectors.toList());
            }

            QueryData queryData = client.currentData();
            if (queryData == null || queryData.getData() == null) {
                return;
            }

            for (List<Object> objects : queryData.getData()) {
                Object[] row = new Object[columns.size()];

                for (int i = 0; i < objects.size(); i++) {
                    String type = queryStatusInfo.getColumns().get(i).getTypeSignature().getRawType();
                    Object value = objects.get(i);
                    if (value != null) {
                        if (type.equals(StandardTypes.TIMESTAMP)) {
                            try {
                                row[i] = LocalDateTime.parse((CharSequence) value, PRESTO_TIMESTAMP_FORMAT).atZone(zone);
                            } catch (Exception e) {
                                LOGGER.error(e, "Error while parsing Presto TIMESTAMP.");
                            }
                        } else if (type.equals(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)) {
                            try {
                                row[i] = ZonedDateTime.parse((CharSequence) value, PRESTO_TIMESTAMP_WITH_TIMEZONE_FORMAT);
                            } catch (Exception e) {
                                LOGGER.error(e, "Error while parsing Presto TIMESTAMP WITH TIMEZONE.");
                            }
                        } else if (type.equals(StandardTypes.DATE)) {
                            row[i] = LocalDate.parse((CharSequence) value);
                        } else {
                            row[i] = objects.get(i);
                        }
                    } else {
                        row[i] = objects.get(i);
                    }
                }

                data.add(Arrays.asList(row));
            }
        }
    }
}
