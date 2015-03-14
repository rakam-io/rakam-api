package org.rakam.report;

import com.facebook.presto.jdbc.internal.airlift.http.client.HttpClientConfig;
import com.facebook.presto.jdbc.internal.airlift.http.client.HttpRequestFilter;
import com.facebook.presto.jdbc.internal.airlift.http.client.Request;
import com.facebook.presto.jdbc.internal.airlift.http.client.jetty.JettyHttpClient;
import com.facebook.presto.jdbc.internal.airlift.http.client.jetty.JettyIoPool;
import com.facebook.presto.jdbc.internal.airlift.http.client.jetty.JettyIoPoolConfig;
import com.facebook.presto.jdbc.internal.airlift.units.Duration;
import com.facebook.presto.jdbc.internal.client.ClientSession;
import com.facebook.presto.jdbc.internal.client.QueryError;
import com.facebook.presto.jdbc.internal.client.QueryResults;
import com.facebook.presto.jdbc.internal.client.StatementClient;
import com.facebook.presto.jdbc.internal.guava.collect.ImmutableSet;
import com.facebook.presto.jdbc.internal.guava.collect.Lists;
import com.facebook.presto.jdbc.internal.guava.net.HostAndPort;
import com.facebook.presto.jdbc.internal.guava.net.HttpHeaders;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.jdbc.internal.airlift.http.client.HttpUriBuilder.uriBuilder;
import static com.facebook.presto.jdbc.internal.airlift.http.client.Request.Builder.fromRequest;
import static com.facebook.presto.jdbc.internal.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.jdbc.internal.guava.base.Preconditions.checkNotNull;

/**
 * Created by buremba <Burak Emre Kabakcı> on 09/03/15 16:10.
 */
public class PrestoExecutor {
    private static final ExecutorService queryExecutor;
    static {
        queryExecutor = new ThreadPoolExecutor(1, 16, 10, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                new ThreadFactoryBuilder()
                .setNameFormat("collection-query-executor")
                .setUncaughtExceptionHandler((t, e) -> e.printStackTrace()).build());
    }

    static JettyHttpClient httpClient = new JettyHttpClient(
            new HttpClientConfig()
                    .setConnectTimeout(new Duration(10, TimeUnit.SECONDS))
                    .setSocksProxy(getSystemSocksProxy()),
            new JettyIoPool("presto-jdbc", new JettyIoPoolConfig()),
            ImmutableSet.of(new UserAgentRequestFilter("deneme")));

    private final PrestoConfig prestoAddress;

    @Inject
    public PrestoExecutor(PrestoConfig prestoConfig) {
        this.prestoAddress = prestoConfig;
    }

    public CompletableFuture<QueryResult> executeQuery(String query) {
        CompletableFuture<QueryResult> future = new CompletableFuture<>();
        StatementClient client = startQuery(query);
        List<List<Object>> results = Lists.newArrayList();
        queryExecutor.execute(new Runnable() {
            @Override
            public void run() {
                if (client.isFailed()) {
                    future.complete(new QueryResult(null, null, client.current().getError()));
                    client.close();
                } else if (!client.isValid()) {
                    Optional.ofNullable(client.finalResults().getData())
                            .ifPresent((newResults) -> newResults.forEach(results::add));

                    List<Column> columns = Lists.newArrayList();
                    List<com.facebook.presto.jdbc.internal.client.Column> internalColumns = client.finalResults().getColumns();
                    for (int i = 0; i < internalColumns.size(); i++) {
                        com.facebook.presto.jdbc.internal.client.Column c = internalColumns.get(i);
                        columns.add(new Column(c.getName(), c.getType(), i + 1));
                    }
                    future.complete(new QueryResult(columns, results, null));
                    client.close();
                } else {
                    Optional.ofNullable(client.current().getData())
                            .ifPresent((newResults) -> newResults.forEach(results::add));
                    client.advance();
                    run();
                }
            }
        });
        return future;
    }

    @Nullable
    private static HostAndPort getSystemSocksProxy()
    {
        URI uri = URI.create("socket://0.0.0.0:80");
        for (Proxy proxy : ProxySelector.getDefault().select(uri)) {
            if (proxy.type() == Proxy.Type.SOCKS) {
                if (proxy.address() instanceof InetSocketAddress) {
                    InetSocketAddress address = (InetSocketAddress) proxy.address();
                    return HostAndPort.fromParts(address.getHostString(), address.getPort());
                }
            }
        }
        return null;
    }

    static class UserAgentRequestFilter
            implements HttpRequestFilter
    {
        private final String userAgent;

        public UserAgentRequestFilter(String userAgent)
        {
            this.userAgent = checkNotNull(userAgent, "userAgent is null");
        }

        @Override
        public Request filterRequest(Request request)
        {
            return fromRequest(request)
                    .addHeader(HttpHeaders.USER_AGENT, userAgent)
                    .build();
        }
    }

    private StatementClient startQuery(String query) {

        HostAndPort hostAndPort = HostAndPort.fromString(prestoAddress.getAddress());
        URI uri = uriBuilder()
                .scheme("http")
                .host(hostAndPort.getHostText())
                .port(hostAndPort.getPort())
                .build();

        ClientSession session = new ClientSession(
                uri,
                "emre",
                "naber",
                "default",
                "default",
                TimeZone.getDefault().getID(),
                Locale.ENGLISH,
                ImmutableMap.of(),
                false);

        return new StatementClient(httpClient, jsonCodec(QueryResults.class), session, query);
    }

    public static class Column {
        public final String name;
        public final String type;
        public final int position;

        @JsonCreator
        public Column(@JsonProperty("name") String name,
                      @JsonProperty("type") String type,
                      @JsonProperty("position") int position) {
            this.name = name;
            this.type = type;
            this.position = position;
        }

        @JsonProperty
        public String getName() {
            return name;
        }

        @JsonProperty
        public String getType() {
            return type;
        }
    }

    public static class QueryResult {
        private final List<Column> columns;
        private final List<List<Object>> result;
        private final QueryError error;

        public QueryResult(List<Column> columns, List<List<Object>> result, QueryError error) {
            this.columns = columns;
            this.result = result;
            this.error = error;
        }

        public List<Column> getColumns() {
            return columns;
        }

        public List<List<Object>> getResult() {
            return result;
        }

        public QueryError getError() {
            return error;
        }
    }

    public static class Stats {
        public final int percentage;
        public final String state;
        public final int node;
        public final long processedRows;
        public final long processedBytes;
        public final long userTime;
        public final long cpuTime;
        public final long wallTime;

        @JsonCreator
        public Stats(@JsonProperty("percentage") int percentage,
                     @JsonProperty("state") String state,
                     @JsonProperty("node") int node,
                     @JsonProperty("processedRows") long processedRows,
                     @JsonProperty("processedBytes") long processedBytes,
                     @JsonProperty("userTime") long userTime,
                     @JsonProperty("cpuTime") long cpuTime,
                     @JsonProperty("wallTime") long wallTime) {
            this.percentage = percentage;
            this.state = state;
            this.node = node;
            this.processedRows = processedRows;
            this.userTime = userTime;
            this.cpuTime = cpuTime;
            this.wallTime = wallTime;
            this.processedBytes = processedBytes;
        }
    }
}
