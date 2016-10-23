package org.rakam.presto.analysis;

import com.facebook.presto.jdbc.internal.airlift.units.Duration;
import com.facebook.presto.jdbc.internal.client.ClientSession;
import com.facebook.presto.rakam.externaldata.DataManager;
import com.facebook.presto.rakam.externaldata.source.RemoteFileDataSource;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Singleton;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.SchemaField;
import org.rakam.presto.analysis.datasource.CustomDataSource;
import org.rakam.presto.analysis.datasource.CustomDataSourceHttpService;
import org.rakam.report.QueryExecutor;
import org.rakam.report.QuerySampling;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;

import javax.inject.Inject;

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
    private ClientSession defaultSession;

    @Inject
    public PrestoQueryExecutor(PrestoConfig prestoConfig, CustomDataSourceHttpService customDataSource, Metastore metastore)
    {
        this.prestoConfig = prestoConfig;
        this.metastore = metastore;
        this.customDataSource = customDataSource;
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
        if (node.getPrefix().isPresent()) {
            String prefix = node.getPrefix().get().toString();
            if (prefix.equals("continuous")) {
                return prestoConfig.getStreamingConnector() + "." +
                        checkCollection(project) + "." +
                        checkCollection(node.getSuffix());
            }
            else if (prefix.equals("materialized")) {
                return getTableReference(project, MATERIALIZED_VIEW_PREFIX + node.getSuffix(), sample);
            }
            else if (!prefix.equals("collection")) {
                try {
                    String encodedKey = sessionParameters.get("external.source_options");
                    Map<String, DataManager.DataSourceType> params;
                    if (encodedKey != null) {
                        params = JsonHelper.read(getDecoder().decode(encodedKey), Map.class);
                    }
                    else {
                        params = new HashMap<>();
                    }

                    if (!params.containsKey(prefix)) {
                        DataManager.DataSourceType dataSourceType;
                        if (prefix.equals("remotefile")) {
                            List<RemoteFileDataSource.RemoteTable> files = customDataSource.getFiles(project);
                            dataSourceType = new DataManager.DataSourceType("REMOTE_FILE", ImmutableMap.of("tables", files));
                        }
                        else {
                            CustomDataSource dataSource = customDataSource.getDatabase(project, prefix);
                            dataSourceType = new DataManager.DataSourceType(dataSource.type, dataSource.options);
                        }

                        params.put(prefix, dataSourceType);
                        sessionParameters.put("external.source_options", getEncoder().encodeToString(encodeAsBytes(params)));
                    }

                    return "external." +
                            checkCollection(prefix) + "." +
                            checkCollection(node.getSuffix());
                }
                catch (RakamException e) {
                    throw new RakamException("Schema does not exist: " + prefix, BAD_REQUEST);
                }
            }
        }

        // special prefix for all columns
        if (node.getSuffix().equals("_all") && !node.getPrefix().isPresent()) {
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
            if (node.getSuffix().equals("users")) {
                return prestoConfig.getUserConnector() + ".users." + project;
            }
            return getTableReference(project, node.getSuffix(), sample);
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
