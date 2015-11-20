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
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Singleton;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.Metastore;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.facebook.presto.jdbc.internal.airlift.http.client.Request.Builder.fromRequest;
import static com.facebook.presto.jdbc.internal.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.jdbc.internal.guava.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static org.rakam.analysis.PrestoMaterializedViewService.MATERIALIZED_VIEW_PREFIX;
import static org.rakam.report.PrestoContinuousQueryService.PRESTO_STREAMING_CATALOG_NAME;

@Singleton
public class PrestoQueryExecutor implements QueryExecutor {
    private final SqlParser parser = new SqlParser();
    private final PrestoConfig prestoConfig;
    private final JettyHttpClient httpClient = new JettyHttpClient(
            new HttpClientConfig()
                    .setConnectTimeout(new Duration(10, TimeUnit.SECONDS))
                    .setSocksProxy(getSystemSocksProxy()), new JettyIoPool("presto-jdbc", new JettyIoPoolConfig()),
            ImmutableSet.of(new UserAgentRequestFilter("rakam")));
    private final Metastore metastore;

    @Inject
    public PrestoQueryExecutor(PrestoConfig prestoConfig, Metastore metastore) {
        this.prestoConfig = prestoConfig;
        this.metastore = metastore;
    }

    public PrestoQueryExecution executeRawQuery(String query) {
        return new PrestoQueryExecution(startQuery(query));
    }

    @Override
    public QueryExecution executeRawStatement(String sqlQuery) {
        return null;
    }

    @Override
    public String formatTableReference(String project, QualifiedName node) {
        if (node.getPrefix().isPresent()) {
            switch (node.getPrefix().get().toString()) {
                case "continuous":
                    return PRESTO_STREAMING_CATALOG_NAME + "." + project + "." + node.getSuffix();
                case "materialized":
                    return project + "." + MATERIALIZED_VIEW_PREFIX + node.getSuffix();
                default:
                    throw new IllegalArgumentException("Schema does not exist: " + node.getPrefix().get().toString());
            }
        }

        // special prefix for all columns
        if (node.getSuffix().equals("_all")) {
            Map<String, List<SchemaField>> collections = metastore.getCollections(project);
            String columns = collections.values().stream()
                    .flatMap(item -> item.stream()).distinct()
                    .map(field -> field.getName()).collect(Collectors.joining(", "));

            return "(" +collections.keySet().stream()
                    .map(collection -> format("select %s from %s", columns.isEmpty() ? "1" : columns, collection))
                    .collect(Collectors.joining(" union all ")) + ")";
        } else {
            QualifiedName prefix = new QualifiedName(prestoConfig.getColdStorageConnector());
            String hotStorageConnector = prestoConfig.getHotStorageConnector();
            String table = project + "." + node.getSuffix();

            if (hotStorageConnector != null) {
                return "(select * from " + prefix.getSuffix() + "." + table + " union all " +
                        "select * from " + hotStorageConnector + "." + table + ")" +
                        " as " + node.getSuffix();
            } else {
                return prefix.getSuffix() + "." + table;
            }
        }
    }

    private StatementClient startQuery(String query) {

        URI uri = prestoConfig.getAddress();

        ClientSession session = new ClientSession(
                uri,
                "rakam",
                "api-server",
                prestoConfig.getColdStorageConnector(),
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
