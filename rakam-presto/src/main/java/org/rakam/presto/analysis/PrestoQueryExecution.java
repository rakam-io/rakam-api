package org.rakam.presto.analysis;

import com.facebook.presto.jdbc.internal.airlift.http.client.HttpClientConfig;
import com.facebook.presto.jdbc.internal.airlift.http.client.HttpRequestFilter;
import com.facebook.presto.jdbc.internal.airlift.http.client.Request;
import com.facebook.presto.jdbc.internal.airlift.http.client.jetty.JettyHttpClient;
import com.facebook.presto.jdbc.internal.airlift.http.client.jetty.JettyIoPool;
import com.facebook.presto.jdbc.internal.airlift.http.client.jetty.JettyIoPoolConfig;
import com.facebook.presto.jdbc.internal.airlift.units.Duration;
import com.facebook.presto.jdbc.internal.client.ClientSession;
import com.facebook.presto.jdbc.internal.client.ClientTypeSignatureParameter;
import com.facebook.presto.jdbc.internal.client.ErrorLocation;
import com.facebook.presto.jdbc.internal.client.QueryResults;
import com.facebook.presto.jdbc.internal.client.StatementClient;
import com.facebook.presto.jdbc.internal.client.StatementStats;
import com.facebook.presto.jdbc.internal.guava.collect.ImmutableSet;
import com.facebook.presto.jdbc.internal.guava.collect.Lists;
import com.facebook.presto.jdbc.internal.guava.net.HostAndPort;
import com.facebook.presto.jdbc.internal.guava.net.HttpHeaders;
import com.facebook.presto.jdbc.internal.spi.type.StandardTypes;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.report.QueryError;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryResult;
import org.rakam.report.QueryStats;
import org.rakam.util.RakamException;
import org.rakam.util.SentryUtil;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.facebook.presto.jdbc.internal.airlift.http.client.Request.Builder.fromRequest;
import static com.facebook.presto.jdbc.internal.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.jdbc.internal.guava.base.Preconditions.checkNotNull;
import static com.facebook.presto.jdbc.internal.spi.type.ParameterKind.TYPE;
import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.rakam.collection.FieldType.*;

public class PrestoQueryExecution implements QueryExecution {
    private final static Logger LOGGER = Logger.get(PrestoQueryExecution.class);
    private static final JettyHttpClient HTTP_CLIENT = new JettyHttpClient(
            new HttpClientConfig()
                    .setConnectTimeout(new Duration(10, SECONDS))
                    .setSocksProxy(getSystemSocksProxy()), new JettyIoPool("presto-jdbc", new JettyIoPoolConfig()),
            ImmutableSet.of(new UserAgentRequestFilter("rakam")));

    // doesn't seem to be a good way but presto client uses a synchronous http client
    // so it blocks the thread when executing queries
    private static final ExecutorService QUERY_EXECUTOR = Executors.newWorkStealingPool();
    private final List<List<Object>> data = Lists.newArrayList();
    private static final com.facebook.presto.jdbc.internal.airlift.json.JsonCodec<QueryResults> QUERY_RESULTS_JSON_CODEC = jsonCodec(QueryResults.class);
    private List<SchemaField> columns;
    private String transactionId;

    private final CompletableFuture<QueryResult> result = new CompletableFuture<>();
    public static final DateTimeFormatter PRESTO_TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    public static final DateTimeFormatter PRESTO_TIMESTAMP_WITH_TIMEZONE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS z");

    private StatementClient client;
    private final Instant startTime;

    public PrestoQueryExecution(ClientSession session, String query) {
        this.startTime = Instant.now();

        try {
            client = new StatementClient(HTTP_CLIENT, QUERY_RESULTS_JSON_CODEC, session, query);
        } catch (RuntimeException e) {
            LOGGER.warn(e, "Presto server is not active.");
            throw new RakamException("Presto server is not active.", HttpResponseStatus.BAD_GATEWAY);
        }
        QUERY_EXECUTOR.execute(new QueryTracker());
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

    public String getTransactionId() {
        return transactionId;
    }

    @Override
    public QueryStats currentStats() {

        if(client.isFailed()) {
            return new QueryStats(QueryStats.State.FAILED);
        }

        StatementStats stats = (!client.isValid() ? client.finalResults() : client.current())
                .getStats();

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

    private static HostAndPort getSystemSocksProxy() {
        URI uri = URI.create("socket://0.0.0.0:80");
        for (Proxy proxy : ProxySelector.getDefault().select(uri)) {
            if (proxy.type() == Proxy.Type.SOCKS &&
                    proxy.address() instanceof InetSocketAddress) {
                InetSocketAddress address = (InetSocketAddress) proxy.address();
                return HostAndPort.fromParts(address.getHostString(), address.getPort());
            }
        }
        return null;
    }

    static class UserAgentRequestFilter
            implements HttpRequestFilter {
        private final String userAgent;

        public UserAgentRequestFilter(String userAgent) {
            this.userAgent = checkNotNull(userAgent, "userAgent is null");
        }

        @Override
        public Request filterRequest(Request request) {
            return fromRequest(request)
                    .addHeader(HttpHeaders.USER_AGENT, userAgent)
                    .build();
        }
    }

    private class QueryTracker implements Runnable {
        @Override
        public void run() {
            try {
                while (client.isValid() && client.advance()) {
                    transformAndAdd(client.current());
                }

                // update transaction ID if necessary
                if (client.isClearTransactionId()) {
                    transactionId = null;
                }
                if (client.getStartedtransactionId() != null) {
                    transactionId = client.getStartedtransactionId();
                }

                if (client.isFailed()) {
                    com.facebook.presto.jdbc.internal.client.QueryError error = client.finalResults().getError();
                    ErrorLocation errorLocation = error.getErrorLocation();
                    QueryError queryError = new QueryError(error.getFailureInfo().getMessage(),
                            error.getSqlState(),
                            error.getErrorCode(),
                            errorLocation != null ? errorLocation.getLineNumber() : null,
                            errorLocation != null ? errorLocation.getColumnNumber() : null);
                    SentryUtil.logQueryError(getQuery(), queryError, PrestoQueryExecutor.class);
                    result.complete(QueryResult.errorResult(queryError));
                } else {
                    transformAndAdd(client.finalResults());

                    ImmutableMap<String, Object> stats = ImmutableMap.of(
                            QueryResult.EXECUTION_TIME, startTime.until(Instant.now(), ChronoUnit.MILLIS));

                    result.complete(new QueryResult(columns, data, stats));
                }
            } catch (Exception e) {
                QueryError queryError = QueryError.create(e.getMessage());
                SentryUtil.logQueryError(getQuery(), queryError, PrestoQueryExecutor.class);
                result.complete(QueryResult.errorResult(queryError));
            }
        }

        private void transformAndAdd(QueryResults result) {
            if (result.getError() != null || result.getColumns() == null) {
                return;
            }

            if (columns == null) {
                columns = result.getColumns().stream()
                        .map(c -> {
                            List<ClientTypeSignatureParameter> arguments = c.getTypeSignature().getArguments();
                            return new SchemaField(c.getName(), fromPrestoType(c.getTypeSignature().getRawType(),
                                    arguments.stream()
                                            .filter(argument -> argument.getKind() == TYPE)
                                            .map(argument -> argument.getTypeSignature().getRawType()).iterator()));
                        })
                        .collect(Collectors.toList());
            }

            if (result.getData() == null) {
                return;
            }

            for (List<Object> objects : result.getData()) {
                Object[] row = new Object[columns.size()];

                for (int i = 0; i < objects.size(); i++) {
                    String type = result.getColumns().get(i).getTypeSignature().getRawType();
                    Object value = objects.get(i);
                    if (value != null) {
                        if (type.equals(StandardTypes.TIMESTAMP)) {
                            try {
                                row[i] = LocalDateTime.parse((CharSequence) value, PRESTO_TIMESTAMP_FORMAT).toInstant(UTC);
                            } catch (Exception e) {
                                LOGGER.error(e, "Error while parsing Presto TIMESTAMP.");
                            }
                        } else if (type.equals(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)) {
                            try {
                                row[i] = LocalDateTime.parse((CharSequence) value, PRESTO_TIMESTAMP_WITH_TIMEZONE_FORMAT).toInstant(UTC);
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
