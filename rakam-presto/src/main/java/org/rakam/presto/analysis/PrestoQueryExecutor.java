package org.rakam.presto.analysis;

import com.facebook.presto.client.ClientSession;
import com.facebook.presto.rakam.externaldata.DataManager.DataSourceType;
import com.facebook.presto.rakam.externaldata.source.MysqlDataSource;
import com.facebook.presto.rakam.externaldata.source.PostgresqlDataSource;
import com.facebook.presto.rakam.externaldata.source.RemoteFileDataSource;
import com.facebook.presto.rakam.externaldata.source.RemoteFileDataSource.CompressionType;
import com.facebook.presto.rakam.externaldata.source.RemoteFileDataSource.ExternalSourceType;
import com.facebook.presto.sql.RakamSqlFormatter;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.QualifiedName;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Singleton;
import io.airlift.units.Duration;
import org.rakam.analysis.datasource.CustomDataSourceService;
import org.rakam.analysis.datasource.RemoteTable;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.SchemaField;
import org.rakam.config.JDBCConfig;
import org.rakam.config.ProjectConfig;
import org.rakam.postgresql.report.PostgresqlQueryExecution;
import org.rakam.presto.PrestoModule.UserConfig;
import org.rakam.analysis.datasource.CustomDataSource;
import org.rakam.analysis.datasource.JDBCSchemaConfig;
import org.rakam.analysis.datasource.SupportedCustomDatabase;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;
import org.rakam.report.QuerySampling;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.net.URI;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static java.lang.String.format;
import static java.util.Base64.getDecoder;
import static java.util.Base64.getEncoder;
import static org.rakam.postgresql.report.PostgresqlQueryExecutor.dbSeparator;
import static org.rakam.presto.PrestoPseudoContinuousQueryService.CONTINUOUS_QUERY_PREFIX;
import static org.rakam.presto.analysis.PrestoMaterializedViewService.MATERIALIZED_VIEW_PREFIX;
import static org.rakam.presto.analysis.PrestoRakamRaptorMetastore.toType;
import static org.rakam.util.JsonHelper.encodeAsBytes;
import static org.rakam.util.ValidationUtil.checkCollection;
import static org.rakam.util.ValidationUtil.checkProject;
import static org.rakam.util.ValidationUtil.checkTableColumn;

@Singleton
public class PrestoQueryExecutor
        implements QueryExecutor
{
    private final PrestoConfig prestoConfig;

    private final Metastore metastore;
    private final CustomDataSourceService customDataSource;
    private final JDBCConfig userJdbcConfig;
    private final ProjectConfig projectConfig;
    private SqlParser sqlParser = new SqlParser();

    @Inject
    public PrestoQueryExecutor(
            ProjectConfig projectConfig,
            PrestoConfig prestoConfig,
            @Nullable CustomDataSourceService customDataSource,
            @Nullable @UserConfig com.google.common.base.Optional<JDBCConfig> userJdbcConfig,
            Metastore metastore)
    {
        this.projectConfig = projectConfig;
        this.prestoConfig = prestoConfig;
        this.metastore = metastore;
        this.customDataSource = customDataSource;
        this.userJdbcConfig = userJdbcConfig == null ? null : userJdbcConfig.orNull();
    }

    @Override
    public PrestoQueryExecution executeRawQuery(String query, ZoneId timezone, Map<String, String> sessionParameters)
    {
        return internalExecuteRawQuery(query, createSession(null, timezone, sessionParameters));
    }

    @Override
    public QueryExecution executeRawQuery(String query, Map<String, String> sessionProperties)
    {
        return executeRawQuery(query, ZoneOffset.UTC, sessionProperties, null);
    }

    @Override
    public QueryExecution executeRawStatement(String query, Map<String, String> sessionProperties)
    {
        return executeRawStatement(query, ZoneOffset.UTC, sessionProperties, null);
    }

    public QueryExecution executeRawStatement(String query, ZoneId timezone, Map<String, String> sessionProperties, String catalog)
    {
        return internalExecuteRawQuery(query, createSession(catalog, timezone, sessionProperties));
    }

    public ClientSession createSession(String catalog, ZoneId timezone, Map<String, String> sessionProperties) {
        return new ClientSession(
                prestoConfig.getAddress(),
                "rakam",
                "api-server",
                null,
                catalog == null ? "default" : catalog,
                "default",
                TimeZone.getTimeZone(timezone == null ? ZoneOffset.UTC : timezone).getID(),
                Locale.ENGLISH,
                sessionProperties,
                null, false, new Duration(1, TimeUnit.MINUTES));
    }

    public QueryExecution executeRawQuery(String query, ZoneId timezone, Map<String, String> sessionProperties, String catalog)
    {
        if (sessionProperties.containsKey("external.source_options")) {
            String encodedKey = sessionProperties.get("external.source_options");
            Map<String, DataSourceType> params;
            if (encodedKey != null) {
                params = JsonHelper.read(getDecoder().decode(encodedKey), new TypeReference<Map<String, DataSourceType>>() {});
            }
            else {
                params = new HashMap<>();
            }

            if (params.size() == 1) {
                Map.Entry<String, DataSourceType> next = params.entrySet().iterator().next();
                QueryExecution singleQueryExecution = getSingleQueryExecution(query, next.getValue());
                if (singleQueryExecution != null) {
                    return singleQueryExecution;
                }
            }
        }

        return executeRawStatement(query, timezone, sessionProperties, catalog);
    }

    private QueryExecution getSingleQueryExecution(String query, DataSourceType type)
    {
        Optional<String> schema;

        SupportedCustomDatabase source;
        try {
            source = SupportedCustomDatabase.getAdapter(type.type);
        }
        catch (IllegalArgumentException e) {
            return null;
        }
        JDBCSchemaConfig convert = JsonHelper.convert(type.data, JDBCSchemaConfig.class);
        char seperator = dbSeparator(type.type);

        switch (type.type) {
            case PostgresqlDataSource.NAME:
            case "REDSHIFT":
                schema = Optional.of(convert.getSchema());
                break;
            case MysqlDataSource.NAME:
                schema = Optional.empty();
                break;
            default:
                schema = Optional.empty();
                break;
        }

        AtomicBoolean hasOutsideReference = new AtomicBoolean();
        StringBuilder builder = new StringBuilder();
        new RakamSqlFormatter.Formatter(builder, qualifiedName -> {
            String prefix = qualifiedName.getPrefix().get().getPrefix().get().toString();
            if (!prefix.equals("external")) {
                hasOutsideReference.set(true);
            }
            else {
                return schema.map(e -> e + "." + qualifiedName.getSuffix())
                        .orElse(qualifiedName.getSuffix());
            }
            return null;
        }, seperator).process(sqlParser.createStatement(query), 1);

        if (hasOutsideReference.get()) {
            return null;
        }

        return new PostgresqlQueryExecution(() -> source.getDataSource().openConnection(convert),
                builder.toString(), false, null);
    }

    public PrestoQueryExecution internalExecuteRawQuery(String query, ClientSession clientSession)
    {
        return new PrestoQueryExecution(clientSession, query);
    }

    @Override
    public String formatTableReference(String project, QualifiedName node, Optional<QuerySampling> sample, Map<String, String> sessionParameters)
    {
        String prefix = node.getPrefix().map(e -> e.toString()).orElse(null);
        String suffix = node.getSuffix();
        if ("continuous".equals(prefix)) {
            if (prestoConfig.getColdStorageConnector().equals("rakam_raptor") && prestoConfig.getEnableStreaming()) {
                return prestoConfig.getStreamingConnector() + "." +
                        checkCollection(project) + "." +
                        checkCollection(suffix);
            }
            else {
                return prestoConfig.getColdStorageConnector() + "." +
                        checkCollection(project) + "." +
                        checkCollection(CONTINUOUS_QUERY_PREFIX + suffix);
            }
        }
        else if ("materialized".equals(prefix)) {
            return getTableReference(project, MATERIALIZED_VIEW_PREFIX + suffix, sample);
        }
        else if ("collection".equals(prefix) || (prefix == null && !"_users".equals(suffix) && !"_all".equals(suffix))) {
            return getTableReference(project, suffix, sample);
        }
        else {
            String encodedKey = sessionParameters.get("external.source_options");
            Map<String, DataSourceType> params;
            if (encodedKey != null) {
                params = JsonHelper.read(getDecoder().decode(encodedKey), Map.class);
            }
            else {
                params = new HashMap<>();
            }

            DataSourceType dataSourceType;

            if (prefix == null && userJdbcConfig != null && suffix.equals("_users")) {
                URI uri = URI.create(userJdbcConfig.getUrl().substring(5));
                JDBCSchemaConfig source = new JDBCSchemaConfig()
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
            // special prefix for all columns
            else if (suffix.equals("_all") && prefix == null) {
                List<Map.Entry<String, List<SchemaField>>> collections = metastore.getCollections(project).entrySet().stream()
                        .filter(c -> !c.getKey().startsWith("_"))
                        .collect(Collectors.toList());
                if (!collections.isEmpty()) {
                    String sharedColumns = collections.get(0).getValue().stream()
                            .filter(col -> collections.stream().allMatch(list -> list.getValue().contains(col)))
                            .map(f -> f.getName())
                            .filter(f -> !f.equals(prestoConfig.getCheckpointColumn()))
                            .collect(Collectors.joining(", "));

                    return "(" + collections.stream().map(Map.Entry::getKey)
                            .map(collection -> format("select '%s' as _collection, %s, %s from %s",
                                    collection,
                                    checkTableColumn(prestoConfig.getCheckpointColumn()),
                                    sharedColumns.isEmpty() ? "1" : sharedColumns,
                                    getTableReference(project, collection, sample)))
                            .collect(Collectors.joining(" union all ")) + ") _all";
                }
                else {
                    return "(select null as \"_collection\", null as _user, null as " + checkTableColumn(projectConfig.getTimeColumn()) + " limit 0) _all";
                }
            }
            else {
                prefix = Optional.ofNullable(prefix).orElse("collection");

                if (customDataSource == null) {
                    throw new RakamException(NOT_FOUND);
                }

                if (prefix.equals("remotefile")) {
                    Map<String, RemoteTable> files = customDataSource.getFiles(project);

                    List<RemoteFileDataSource.RemoteTable> prestoTables = files.entrySet().stream().map(file -> {
                        List<RemoteFileDataSource.Column> collect = file.getValue().columns.stream()
                                .map(column -> new RemoteFileDataSource.Column(column.getName(), toType(column.getType())))
                                .collect(Collectors.toList());

                        return new RemoteFileDataSource.RemoteTable(file.getKey(),
                                file.getValue().url,
                                file.getValue().indexUrl,
                                file.getValue().typeOptions,
                                collect,
                                Optional.ofNullable(file.getValue().compressionType).map(value -> CompressionType.valueOf(value.name())).orElse(null),
                                Optional.ofNullable(file.getValue().format).map(value -> ExternalSourceType.valueOf(value.name())).orElse(null));
                    }).collect(Collectors.toList());

                    dataSourceType = new DataSourceType("REMOTE_FILE", ImmutableMap.of("tables", prestoTables));
                }
                else {
                    CustomDataSource dataSource;
                    try {
                        dataSource = customDataSource.getDatabase(project, prefix);
                    }
                    catch (RakamException e) {
                        if (e.getStatusCode() == NOT_FOUND) {
                            throw new RakamException("Schema does not exist: " + prefix, BAD_REQUEST);
                        }
                        throw e;
                    }
                    dataSourceType = new DataSourceType(dataSource.type, dataSource.options);
                }
            }

            if (dataSourceType != null) {
                params.put(prefix, dataSourceType);
                sessionParameters.put("external.source_options", getEncoder().encodeToString(encodeAsBytes(params)));
            }

            return "external." + checkCollection(prefix) + "." + checkCollection(suffix, dbSeparator(suffix));
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
