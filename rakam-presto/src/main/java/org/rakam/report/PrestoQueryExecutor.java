package org.rakam.report;

import com.facebook.presto.jdbc.internal.airlift.http.client.HttpClientConfig;
import com.facebook.presto.jdbc.internal.airlift.http.client.HttpRequestFilter;
import com.facebook.presto.jdbc.internal.airlift.http.client.Request;
import com.facebook.presto.jdbc.internal.airlift.http.client.jetty.JettyHttpClient;
import com.facebook.presto.jdbc.internal.airlift.http.client.jetty.JettyIoPool;
import com.facebook.presto.jdbc.internal.airlift.http.client.jetty.JettyIoPoolConfig;
import com.facebook.presto.jdbc.internal.airlift.units.Duration;
import com.facebook.presto.jdbc.internal.client.ClientSession;
import com.facebook.presto.jdbc.internal.client.QueryResults;
import com.facebook.presto.jdbc.internal.client.StatementClient;
import com.facebook.presto.jdbc.internal.guava.collect.ImmutableSet;
import com.facebook.presto.jdbc.internal.guava.net.HostAndPort;
import com.facebook.presto.jdbc.internal.guava.net.HttpHeaders;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Singleton;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.URI;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.jdbc.internal.airlift.http.client.HttpUriBuilder.uriBuilder;
import static com.facebook.presto.jdbc.internal.airlift.http.client.Request.Builder.fromRequest;
import static com.facebook.presto.jdbc.internal.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.jdbc.internal.guava.base.Preconditions.checkNotNull;

/**
 * Created by buremba <Burak Emre Kabakcı> on 09/03/15 16:10.
 */
@Singleton
public class PrestoQueryExecutor implements QueryExecutor {

    private final JettyHttpClient httpClient = new JettyHttpClient(
            new HttpClientConfig()
                    .setConnectTimeout(new Duration(10, TimeUnit.SECONDS))
                    .setSocksProxy(getSystemSocksProxy()),
            new JettyIoPool("presto-jdbc", new JettyIoPoolConfig()),
            ImmutableSet.of(new UserAgentRequestFilter("rakam")));

    private final PrestoConfig prestoAddress;

    @Inject
    public PrestoQueryExecutor(PrestoConfig prestoConfig) {
        this.prestoAddress = prestoConfig;
    }

    public PrestoQueryExecution executeQuery(String query) {
        return new PrestoQueryExecution(startQuery(prestoAddress, query));
    }

    @Override
    public QueryExecution executeStatement(String sqlQuery) {
        return executeQuery(sqlQuery);
    }

    private StatementClient startQuery(PrestoConfig config, String query) {

        HostAndPort hostAndPort = HostAndPort.fromString(config.getAddress());
        URI uri = uriBuilder()
                .scheme("http")
                .host(hostAndPort.getHostText())
                .port(hostAndPort.getPort())
                .build();

        ClientSession session = new ClientSession(
                uri,
                "rakam",
                "api-server",
                config.getColdStorageConnector(),
                "default",
                TimeZone.getDefault().getID(),
                Locale.ENGLISH,
                ImmutableMap.of(),
                false);

        return new StatementClient(httpClient, jsonCodec(QueryResults.class), session, query);
    }

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



}
