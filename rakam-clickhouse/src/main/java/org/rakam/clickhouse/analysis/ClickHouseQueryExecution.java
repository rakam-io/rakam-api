package org.rakam.clickhouse.analysis;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.CharStreams;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.http.client.jetty.JettyIoPool;
import io.airlift.http.client.jetty.JettyIoPoolConfig;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.rakam.clickhouse.ClickHouseConfig;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryResult;
import org.rakam.report.QueryStats;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;

import javax.ws.rs.core.UriBuilder;
import java.io.DataInput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_GATEWAY;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.rakam.report.QueryStats.State.FINISHED;
import static org.rakam.report.QueryStats.State.RUNNING;

public class ClickHouseQueryExecution
        implements QueryExecution {
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");
    protected static final JettyHttpClient HTTP_CLIENT = new JettyHttpClient(
            new HttpClientConfig()
                    .setConnectTimeout(new Duration(10, SECONDS))
                    .setSocksProxy(getSystemSocksProxy()), new JettyIoPool("rakam-clickhouse", new JettyIoPoolConfig()),
            ImmutableSet.of());
    private static final Logger LOGGER = Logger.get(ClickHouseQueryExecution.class);
    private static Pattern CLICKHOUSE_TYPE_PATTERN = Pattern.compile("^([A-Za-z0-9]+)\\(([A-Za-z0-9]+)\\)$");
    private final CompletableFuture<ClickHouseQueryResult> result;
    private final String query;
    private final String queryId;
    private final ClickHouseConfig config;

    public ClickHouseQueryExecution(ClickHouseConfig config, String query) {
        this.query = query;
        this.queryId = UUID.randomUUID().toString();
        this.config = config;
        URI uri = UriBuilder
                .fromUri(config.getAddress())
                .queryParam("query_id", queryId).build();

        result = convertCompletableFuture(HTTP_CLIENT.executeAsync(
                Request.builder()
                        .setUri(uri)
                        .setMethod("POST")
                        .setBodyGenerator(createStaticBodyGenerator(query + " format " + QueryResponseHandler.FORMAT, UTF_8))
                        .build(),
                new QueryResponseHandler()));
    }

    public static String runStatement(ClickHouseConfig config, String query) {
        StringResponseHandler.StringResponse run = runStatementSafe(config, query);
        if (run.getStatusCode() != 200) {
            throw new RakamException("Error executing query: " + run.getBody(), INTERNAL_SERVER_ERROR);
        }
        return run.getBody();
    }

    public static StringResponseHandler.StringResponse runStatementSafe(ClickHouseConfig config, String query) {
        URI uri = UriBuilder
                .fromUri(config.getAddress()).queryParam("query", query).build();

        return HTTP_CLIENT.execute(Request.builder()
                        .setUri(uri).setMethod("POST").build(),
                StringResponseHandler.createStringResponseHandler());
    }

    public static HostAndPort getSystemSocksProxy() {
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

    static <T> CompletableFuture<T> convertCompletableFuture(final ListenableFuture<T> listenableFuture) {
        CompletableFuture<T> completable = new CompletableFuture<T>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                // propagate cancel to the listenable future
                boolean result = listenableFuture.cancel(mayInterruptIfRunning);
                super.cancel(mayInterruptIfRunning);
                return result;
            }
        };

        listenableFuture.addListener(() -> {
            try {
                completable.complete(listenableFuture.get());
            } catch (InterruptedException e) {
                completable.completeExceptionally(e);
            } catch (ExecutionException e) {
                completable.completeExceptionally(e.getCause());
            }
        }, Runnable::run);
        return completable;
    }

    public static List<Object> readRowBinary(DataInput input, List<String> types)
            throws IOException {
        ArrayList<Object> list = new ArrayList<>(types.size());
        for (String s : types) {
            Object value;
            switch (s) {
                case "Enum":
                    throw new UnsupportedOperationException();
                case "UInt64":
                case "Int64":
                    value = input.readLong();
                    break;
                case "UInt16":
                case "Int16":
                    value = input.readShort();
                    break;
                case "Float32":
                    value = input.readFloat();
                    break;
                case "Float64":
                    value = input.readDouble();
                    break;
                case "String":
                    byte[] bytes = new byte[readVarInt(input)];
                    input.readFully(bytes);
                    value = new String(bytes, UTF_8);
                    break;
                case "Int32":
                case "UInt32":
                    value = input.readInt();
                    break;
                case "UInt8":
                case "Int8":
                    value = (int) input.readByte();
                    break;
                case "DateTime":
                    value = Instant.ofEpochMilli(input.readInt() * 1000);
                    break;
                case "Date":
                    value = LocalDate.ofEpochDay(input.readShort());
                    break;
                default:
                    Matcher matcher = Pattern.compile("([A-Za-z0-9]+)\\(([0-9]+)\\)$").matcher(s);
                    if (matcher.find()) {
                        String type = matcher.group(1);
                        String group = matcher.group(2);
                        int variable = Integer.parseInt(group);
                        switch (type) {
                            case "FixedString":
                                bytes = new byte[variable];
                                input.readFully(bytes);
                                value = new String(bytes, UTF_8);
                                break;
                            case "Array":
                                for (int i = 0; i < readVarInt(input); i++) {

                                }
                            default:
                                throw new IllegalStateException();
                        }
                        // Nested
                    } else {
                        throw new IllegalStateException();
                    }
            }

            list.add(value);
        }

        return list;
    }

    public static FieldType parseClickhouseType(String type) {
        switch (type) {
            case "Enum":
                return FieldType.STRING;
            case "UInt64":
            case "Int64":
                return FieldType.LONG;
            case "UInt16":
            case "Int16":
                return FieldType.INTEGER;
            case "Float32":
                return FieldType.DOUBLE;
            case "Float64":
                return FieldType.DOUBLE;
            case "String":
                return FieldType.STRING;
            case "Int32":
            case "UInt32":
            case "UInt8":
            case "Int8":
                return FieldType.INTEGER;
            case "DateTime":
                return FieldType.TIMESTAMP;
            case "Date":
                return FieldType.DATE;
            default:
                Matcher matcher = CLICKHOUSE_TYPE_PATTERN.matcher(type);
                if (matcher.find()) {
                    String actualType = matcher.group(1);
                    String group = matcher.group(2);
                    switch (actualType) {
                        case "FixedString":
                            return FieldType.STRING;
                        case "Array":
                            return parseClickhouseType(group).convertToArrayType();
                        case "Nested":
                            return FieldType.MAP_STRING;
                        default:
                            throw new IllegalStateException("The parametrized type cannot be identified: " + type);
                    }
                } else {
                    throw new IllegalStateException("The type cannot be identified: " + type);
                }
        }
    }

    public static int readVarInt(DataInput input)
            throws IOException {
        int size = 0;
        for (int i = 0; ; i += 7) {
            byte tmp = input.readByte();
            if ((tmp & 0x80) == 0 && (i != 4 * 7 || tmp < 1 << 3)) {
                return size | (tmp << i);
            } else if (i < 4 * 7) {
                size |= (tmp & 0x7f) << i;
            } else {
                throw new IllegalStateException("Not the varint representation of a signed int32");
            }
        }
    }

    @Override
    public QueryStats currentStats() {
        if (result.isDone()) {
            return new QueryStats(100, FINISHED, null, null, null, null, null, null);
        } else {
            String status = runStatement(config, format("select rows_read, bytes_read, total_rows_approx, memory_usage from system.processes where query_id = '%s' format "
                    + QueryResponseHandler.FORMAT, queryId));
            if (status.isEmpty()) {
                if (result.isDone()) {
                    return currentStats();
                } else {
                    return new QueryStats(null, RUNNING, null, null, null, null, null, null);
                }
            }
            ClickHouseQueryResult read = JsonHelper.read(status, ClickHouseQueryResult.class);
            int percentage = (((Number) read.data.get(0)).intValue() * 100) / ((Number) read.data.get(2)).intValue();
            return new QueryStats(percentage, RUNNING, null, ((Number) read.data.get(1)).longValue(), ((Number) read.data.get(2)).longValue(),
                    null, null, null);
        }
    }

    @Override
    public boolean isFinished() {
        return result.isDone();
    }

    private QueryResult getSuccessfulQueryResult(ClickHouseQueryResult queryResult) {
        List<SchemaField> columns = queryResult.meta.stream().map(f -> new SchemaField(f.name, parseClickhouseType(f.type)))
                .collect(Collectors.toList());

        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        if (queryResult.totals != null) {
            builder.put("totals", queryResult.totals);
        }
        if (queryResult.extremes != null) {
            builder.put("extremes", queryResult.extremes);
        }

        return new QueryResult(columns, transformResultData(columns, queryResult.data),
                builder.build());
    }

    private List<List<Object>> transformResultData(List<SchemaField> columns, List<List<Object>> data) {
        for (List<Object> objects : data) {
            for (int i = 0; i < columns.size(); i++) {
                String value = objects.get(i).toString();
                if (value == null) {
                    continue;
                }

                FieldType type = columns.get(i).getType();
                switch (type) {
                    case STRING:
                        break;
                    case LONG:
                        objects.set(i, Long.parseLong(value));
                        break;
                    case INTEGER:
                        objects.set(i, Integer.parseInt(value));
                        break;
                    case BOOLEAN:
                        objects.set(i, value.equals("true"));
                        break;
                    case TIMESTAMP:
                        objects.set(i, LocalDateTime.parse(value, DATE_TIME_FORMATTER).toInstant(UTC));
                        break;
                    case DATE:
                        objects.set(i, LocalDate.parse(value));
                        break;
                    case TIME:
                        objects.set(i, LocalTime.parse(value, TIME_FORMATTER));
                        break;
                    case BINARY:
                        objects.set(i, value.getBytes(UTF_8));
                        break;
                    case DECIMAL:
                    case DOUBLE:
                        objects.set(i, Double.parseDouble(value));
                        break;
                    default:
                        if (type.isArray()) {

                        }

                        if (type.isMap()) {

                        }
                }
            }
        }

        return data;
    }

    @Override
    public CompletableFuture<QueryResult> getResult() {
        return result.thenApply(this::getSuccessfulQueryResult);
    }

    @Override
    public void kill() {
        if (!result.isDone()) {
            result.cancel(false);
        }
    }

    private static class QueryResponseHandler
            implements ResponseHandler<ClickHouseQueryResult, RuntimeException> {
        private static final String FORMAT = "JSONCompact";

        @Override
        public ClickHouseQueryResult handleException(Request request, Exception exception)
                throws RuntimeException {
            LOGGER.error(exception);
            throw new RakamException(exception.getMessage(), INTERNAL_SERVER_ERROR);
        }

        @Override
        public ClickHouseQueryResult handle(Request request, Response response)
                throws RuntimeException {
            if (response.getStatusCode() != 200) {
                try {
                    String message = CharStreams.toString(new InputStreamReader(response.getInputStream()));
                    message = message.split(", Stack trace:\n", 2)[0];
                    throw new RakamException(message, BAD_GATEWAY);
                } catch (IOException e) {
                    throw new RakamException("An error occurred", BAD_GATEWAY);
                }
            }

            try {
                return JsonHelper.read(response.getInputStream(), ClickHouseQueryResult.class);
            } catch (IOException e) {
                LOGGER.error(e, "An error occurred while reading query results");
                throw new RakamException("An error occurred while reading query results: " + e.getMessage(),
                        INTERNAL_SERVER_ERROR);
            }
        }
    }
}
