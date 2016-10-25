package org.rakam.presto.analysis;

import com.facebook.presto.jdbc.internal.airlift.units.Duration;
import com.facebook.presto.jdbc.internal.client.ClientSession;
import com.facebook.presto.rakam.externaldata.DataManager.DataSourceType;
import com.facebook.presto.rakam.externaldata.JDBCSchemaConfig;
import com.facebook.presto.rakam.externaldata.source.PostgresqlDataSource;
import com.facebook.presto.rakam.externaldata.source.RemoteFileDataSource;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Singleton;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.SchemaField;
import org.rakam.config.JDBCConfig;
import org.rakam.presto.PrestoModule;
import org.rakam.presto.PrestoModule.UserConfig;
import org.rakam.presto.analysis.datasource.CustomDataSource;
import org.rakam.presto.analysis.datasource.CustomDataSourceHttpService;
import org.rakam.report.QueryExecutor;
import org.rakam.report.QuerySampling;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.facebook.presto.jdbc.internal.client.ClientSession.withTransactionId;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.util.Base64.getDecoder;
import static java.util.Base64.getEncoder;
import static org.rakam.presto.analysis.PrestoMaterializedViewService.MATERIALIZED_VIEW_PREFIX;
import static org.rakam.util.JsonHelper.encodeAsBytes;
import static org.rakam.util.ValidationUtil.checkCollection;

@Singleton
public class PrestoQueryExecutor
        implements QueryExecutor
{
    private final PrestoConfig prestoConfig;

    private final Metastore metastore;
    private final CustomDataSourceHttpService customDataSource;
    private final JDBCConfig userJdbcConfig;
    private ClientSession defaultSession;

    @Inject
    public PrestoQueryExecutor(
            PrestoConfig prestoConfig,
            @Nullable CustomDataSourceHttpService customDataSource,
            @Nullable @UserConfig com.google.common.base.Optional<JDBCConfig> userJdbcConfig,
            Metastore metastore)
    {
        this.prestoConfig = prestoConfig;
        this.metastore = metastore;
        this.customDataSource = customDataSource;
        this.userJdbcConfig = userJdbcConfig == null ? null : userJdbcConfig.orNull();
        this.defaultSession = new ClientSession(
                prestoConfig.getAddress(),
                "rakam",
                "api-server",
                prestoConfig.getColdStorageConnector(),
                "default",
                TimeZone.getTimeZone(UTC).getID(),
                Locale.ENGLISH,
                ImmutableMap.of(),
                null,
                false, new Duration(1, TimeUnit.MINUTES));
    }

    @Override
    public PrestoQueryExecution executeRawQuery(String query)
    {
        return new PrestoQueryExecution(defaultSession, query);
    }

    @Override
    public PrestoQueryExecution executeRawQuery(String query, Map<String, String> sessionProperties)
    {
        return executeRawQuery(query, sessionProperties, null);
    }

    public PrestoQueryExecution executeRawQuery(String query, String transactionId)
    {
        return new PrestoQueryExecution(withTransactionId(defaultSession, transactionId), query);
    }

    public PrestoQueryExecution executeRawQuery(String query, Map<String, String> sessionProperties, String catalog)
    {
        return new PrestoQueryExecution(new ClientSession(
                prestoConfig.getAddress(),
                "rakam",
                "api-server",
                catalog == null ? "default" : catalog,
                "default",
                TimeZone.getDefault().getID(),
                Locale.ENGLISH,
                sessionProperties,
                null, false, new Duration(1, TimeUnit.MINUTES)), query);
    }

    @Override
    public PrestoQueryExecution executeRawStatement(String sqlQuery)
    {
        return executeRawQuery(sqlQuery);
    }

    @Override
    public String formatTableReference(String project, QualifiedName node, Optional<QuerySampling> sample)
    {
        return formatTableReference(project, node, sample, ImmutableMap.of());
    }

    @Override
    public String formatTableReference(String project, QualifiedName node, Optional<QuerySampling> sample, Map<String, String> sessionParameters)
    {
        String prefix = node.getPrefix().map(e -> e.toString()).orElse(null);
        String suffix = node.getSuffix();
        if ("continuous".equals(prefix)) {
            return prestoConfig.getStreamingConnector() + "." +
                    checkCollection(project) + "." +
                    checkCollection(suffix);
        }
        else if ("materialized".equals(prefix)) {
            return getTableReference(project, MATERIALIZED_VIEW_PREFIX + suffix, sample);
        }
        else if (!"collection".equals(prefix)) {
            try {
                String encodedKey = sessionParameters.get("external.source_options");
                Map<String, DataSourceType> params;
                if (encodedKey != null) {
                    params = JsonHelper.read(getDecoder().decode(encodedKey), Map.class);
                }
                else {
                    params = new HashMap<>();
                }

                DataSourceType dataSourceType = null;

                if (prefix == null && userJdbcConfig != null && suffix.equals("users")) {
                    URI uri = URI.create(userJdbcConfig.getUrl().substring(5));
                    JDBCSchemaConfig source = new PostgresqlDataSource.PostgresqlDataSourceFactory()
                            .setDatabase(uri.getPath().substring(1).split("\\?", 2)[0])
                            .setHost(uri.getHost())
                            .setUsername(userJdbcConfig.getUsername())
                            .setPassword(userJdbcConfig.getPassword())
                            .setSchema("users");

                    prefix = "users";
                    suffix = project;
                    CustomDataSource dataSource = new CustomDataSource("POSTGRESQL", "users", source);
                    dataSourceType = new DataSourceType(dataSource.type, dataSource.options);
                }
                else if (!params.containsKey(prefix) && prefix != null) {
                    if(customDataSource == null) {
                        throw new RakamException(NOT_FOUND);
                    }
                    if (prefix.equals("remotefile")) {
                        List<RemoteFileDataSource.RemoteTable> files = customDataSource.getFiles(project);
                        dataSourceType = new DataSourceType("REMOTE_FILE", ImmutableMap.of("tables", files));
                    }
                    else {
                        CustomDataSource dataSource = customDataSource.getDatabase(project, prefix);
                        dataSourceType = new DataSourceType(dataSource.type, dataSource.options);
                    }
                }

                if (dataSourceType != null) {
                    params.put(prefix, dataSourceType);
                    sessionParameters.put("external.source_options", getEncoder().encodeToString(encodeAsBytes(params)));
                }

                if (prefix != null) {
                    return "external." +
                            checkCollection(prefix) + "." +
                            checkCollection(suffix);
                }
            }
            catch (RakamException e) {
                throw new RakamException("Schema does not exist: " + prefix, BAD_REQUEST);
            }
        }

        // special prefix for all columns
        if (suffix.equals("_all") && !node.getPrefix().isPresent()) {
            List<Map.Entry<String, List<SchemaField>>> collections = metastore.getCollections(project).entrySet().stream()
                    .filter(c -> !c.getKey().startsWith("_"))
                    .collect(Collectors.toList());
            if (!collections.isEmpty()) {
                String sharedColumns = collections.get(0).getValue().stream()
                        .filter(col -> collections.stream().allMatch(list -> list.getValue().contains(col)))
                        .map(f -> f.getName())
                        .collect(Collectors.joining(", "));

                return "(" + collections.stream().map(Map.Entry::getKey)
                        .map(collection -> format("select '%s' as \"$collection\", %s from %s",
                                collection,
                                sharedColumns.isEmpty() ? "1" : sharedColumns,
                                getTableReference(project, collection, sample)))
                        .collect(Collectors.joining(" union all ")) + ") _all";
            }
            else {
                return "(select null as \"$collection\", null as _user, null as _time limit 0) _all";
            }
        }
        else {
            return getTableReference(project, suffix, sample);
        }
    }

    private String getTableReference(String project, String tableName, Optional<QuerySampling> sample)
    {
        String hotStorageConnector = prestoConfig.getHotStorageConnector();
        String table = checkCollection(project) + "." + checkCollection(tableName) +
                sample.map(e -> " TABLESAMPLE " + e.method.name() + "(" + e.percentage + ")").orElse("");

        if (hotStorageConnector != null) {
            return "((select * from " + prestoConfig.getColdStorageConnector() + "." + table + " union all " +
                    "select * from " + hotStorageConnector + "." + table + ")" +
                    " as " + tableName + ")";
        }
        else {
            return prestoConfig.getColdStorageConnector() + "." + table;
        }
    }
}
