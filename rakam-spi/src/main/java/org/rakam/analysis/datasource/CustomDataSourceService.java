package org.rakam.analysis.datasource;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.Parameter;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.util.AlreadyExistsException;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;
import org.rakam.util.SuccessMessage;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static java.util.Objects.requireNonNull;
import static org.rakam.util.JDBCUtil.fromSql;

public class CustomDataSourceService {
    private final DBI dbi;
    private final ExecutorService executor;

    @Inject
    public CustomDataSourceService(@Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource) {
        this.dbi = new DBI(dataSource);
        this.executor = Executors.newCachedThreadPool();
    }

    @PostConstruct
    public void setup() {
        try (Handle handle = dbi.open()) {
            handle.createStatement("CREATE TABLE IF NOT EXISTS custom_data_source (" +
                    "  project VARCHAR(255) NOT NULL," +
                    "  schema_name VARCHAR(255) NOT NULL," +
                    "  type TEXT NOT NULL," +
                    "  options TEXT," +
                    "  PRIMARY KEY (project, schema_name)" +
                    "  )")
                    .execute();

            handle.createStatement("CREATE TABLE IF NOT EXISTS custom_file_source (" +
                    "  project VARCHAR(255) NOT NULL," +
                    "  table_name VARCHAR(255) NOT NULL," +
                    "  options TEXT," +
                    "  PRIMARY KEY (project, table_name)" +
                    "  )")
                    .execute();
        }
    }

    public CustomDataSourceList listDatabases(String project) {
        try (Handle handle = dbi.open()) {
            List<CustomDataSource> customDataSources = handle.createQuery("SELECT schema_name, type, options FROM custom_data_source WHERE project = :project")
                    .bind("project", project)
                    .map((index, r, ctx) -> {
                        JDBCSchemaConfig read = JsonHelper.read(r.getString(3), JDBCSchemaConfig.class);
                        return new CustomDataSource(r.getString(2), r.getString(1), read);
                    }).list();

            HashMap<String, RemoteTable> files = new HashMap<>();

            handle.createQuery("SELECT table_name, options FROM custom_file_source WHERE project = :project")
                    .bind("project", project)
                    .map((index, r, ctx) -> {
                        files.put(r.getString(1), JsonHelper.read(r.getString(2), RemoteTable.class));
                        return null;
                    }).list();

            return new CustomDataSourceList(customDataSources, files);
        }
    }

    public CompletableFuture<List<SchemaField>> schemaTable(String project, String schema, String table) {
        CustomDataSource customDataSource = getDatabase(project, schema);
        List<SchemaField> builder = new ArrayList<>();

        return CompletableFuture.supplyAsync(() -> {
            SupportedCustomDatabase source = SupportedCustomDatabase.getAdapter(customDataSource.type);
            try (Connection conn = source.getDataSource().openConnection(customDataSource.options)) {
                ResultSet dbColumns = conn.getMetaData().getColumns(null, customDataSource.options.getSchema(), table, null);

                while (dbColumns.next()) {
                    String columnName = dbColumns.getString("COLUMN_NAME");
                    FieldType fieldType;
                    try {
                        fieldType = fromSql(dbColumns.getInt("DATA_TYPE"), dbColumns.getString("TYPE_NAME"));
                    } catch (UnsupportedOperationException e) {
                        continue;
                    }
                    builder.add(new SchemaField(columnName, fieldType));
                }

                return builder;
            } catch (SQLException e) {
                throw Throwables.propagate(e);
            }
        }, executor);
    }

    public CompletableFuture<Map<String, List<String>>> schemaDatabases(String project) {
        return CompletableFuture.supplyAsync(() -> {

            ImmutableMap.Builder<String, List<String>> schemas = ImmutableMap.builder();

            CustomDataSourceList customDataSourceList = listDatabases(project);
            for (CustomDataSource customDataSource : customDataSourceList.customDataSources) {
                List<String> builder = new ArrayList<>();

                SupportedCustomDatabase source = SupportedCustomDatabase.getAdapter(customDataSource.type);
                try (Connection conn = source.getDataSource().openConnection(customDataSource.options)) {
                    ResultSet dbColumns = conn.getMetaData().getTables(null, customDataSource.options.getSchema(), null, null);

                    while (dbColumns.next()) {
                        if (!"TABLE".equals(dbColumns.getString("table_type"))) {
                            continue;
                        }
                        builder.add(dbColumns.getString("table_name"));
                    }
                } catch (SQLException e) {
                    // TODO: report error
                    continue;
                }

                schemas.put(customDataSource.schemaName, builder);
            }

            if (!customDataSourceList.customFileSources.isEmpty()) {
                Map<String, List<SchemaField>> builder = new HashMap<>();
                for (Map.Entry<String, RemoteTable> customFileSource : customDataSourceList.customFileSources.entrySet()) {
                    builder.put(customFileSource.getKey(),
                            Optional.ofNullable(customFileSource.getValue().columns)
                                    .orElse(ImmutableList.of()));
                }
            }

            return schemas.build();
        }, executor);
    }

    public CustomDataSource getDatabase(String project, String schema) {
        try (Handle handle = dbi.open()) {
            Query<Map<String, Object>> bind = handle.createQuery("SELECT type, options FROM custom_data_source WHERE project = :project AND lower(schema_name) = :schema_name")
                    .bind("project", project)
                    .bind("schema_name", schema.toLowerCase(Locale.ENGLISH));

            CustomDataSource first = bind.map((index, r, ctx) ->
                    new CustomDataSource(r.getString(1), schema, JsonHelper.read(r.getString(2), JDBCSchemaConfig.class))).first();

            if (first == null) {
                throw new RakamException(NOT_FOUND);
            }

            return first;
        }
    }

    public RemoteTable getFile(String project, String tableName) {
        try (Handle handle = dbi.open()) {
            Query<Map<String, Object>> bind = handle.createQuery("SELECT options FROM custom_file_source WHERE project = :project AND table_name = :table_name")
                    .bind("project", project)
                    .bind("table_name", tableName);

            RemoteTable first = bind.map((index, r, ctx) -> {
                return JsonHelper.read(r.getString(1), RemoteTable.class);
            }).first();

            if (first == null) {
                throw new RakamException(NOT_FOUND);
            }

            return first;
        }
    }

    public Map<String, RemoteTable> getFiles(String project) {
        try (Handle handle = dbi.open()) {
            Query<Map<String, Object>> bind = handle.createQuery("SELECT table_name, options FROM custom_file_source WHERE project = :project")
                    .bind("project", project);

            HashMap<String, RemoteTable> map = new HashMap<>();

            bind.map((index, r, ctx) -> {
                map.put(r.getString(1), JsonHelper.read(r.getString(2), RemoteTable.class));
                return null;
            }).list();

            return map;
        }
    }

    public SuccessMessage addDatabase(String project, CustomDataSource hook) {
        try (Handle handle = dbi.open()) {
            try {
                handle.createStatement("INSERT INTO custom_data_source (project, schema_name, type, options) " +
                        "VALUES (:project, :schema_name, :type, :options)")
                        .bind("project", project)
                        .bind("schema_name", hook.schemaName)
                        .bind("type", hook.type)
                        .bind("options", JsonHelper.encode(hook.options))
                        .execute();
                return SuccessMessage.success();
            } catch (Exception e) {
                try {
                    getDatabase(project, hook.schemaName);
                    throw new AlreadyExistsException("Custom database", BAD_REQUEST);
                } catch (RakamException e1) {
                    if (e1.getStatusCode() != NOT_FOUND) {
                        throw e1;
                    } else {
                        throw e;
                    }
                }
            }
        }
    }

    public SuccessMessage addFile(String project, @ApiParam("tableName") String tableName, @ApiParam("options") DiscoverableRemoteTable hook) {
        try (Handle handle = dbi.open()) {
            try {
                handle.createStatement("INSERT INTO custom_file_source (project, table_name, options) " +
                        "VALUES (:project, :table_name, :options)")
                        .bind("project", project)
                        .bind("table_name", tableName)
                        .bind("options", JsonHelper.encode(hook.getTable()))
                        .execute();
                return SuccessMessage.success();
            } catch (Exception e) {
                try {
                    getFile(project, tableName);
                    throw new AlreadyExistsException("Custom file", BAD_REQUEST);
                } catch (RakamException e1) {
                    if (e1.getStatusCode() != NOT_FOUND) {
                        throw e1;
                    } else {
                        throw e;
                    }
                }
            }
        }
    }

    public SuccessMessage removeFile(String project, String tableName) {
        try (Handle handle = dbi.open()) {
            int execute = handle.createStatement("DELETE FROM custom_file_source WHERE project = :project AND table_name = :table_name")
                    .bind("project", project)
                    .bind("table_name", tableName)
                    .execute();

            if (execute == 0) {
                throw new RakamException("Custom file not found", NOT_FOUND);
            }

            return SuccessMessage.success();
        }
    }

    public SuccessMessage removeDatabase(String project, String schemaName) {
        try (Handle handle = dbi.open()) {
            int execute = handle.createStatement("DELETE FROM custom_data_source WHERE project = :project AND schema_name = :schema_name")
                    .bind("project", project)
                    .bind("schema_name", schemaName)
                    .execute();

            if (execute == 0) {
                throw new RakamException("Custom database not found", NOT_FOUND);
            }

            return SuccessMessage.success();
        }
    }

    public SuccessMessage testDatabase(String project, String type, JDBCSchemaConfig options) {
        SupportedCustomDatabase optionalFunction = SupportedCustomDatabase.getAdapter(type);
        Optional<String> test = optionalFunction.getDataSource().test(options);
        if (test.isPresent()) {
            throw new RakamException(test.get(), BAD_REQUEST);
        }

        return SuccessMessage.success();
    }

    public SuccessMessage testFile(String project, DiscoverableRemoteTable hook) {
        ExternalFileCustomDataSource source = new ExternalFileCustomDataSource();
        Optional<String> test = source.test(hook.getTable());
        if (test.isPresent()) {
            throw new RakamException(test.get(), BAD_REQUEST);
        }

        return SuccessMessage.success();
    }

    public static class CustomDataSourceList {
        public final List<CustomDataSource> customDataSources;
        public final Map<String, RemoteTable> customFileSources;

        @JsonCreator
        public CustomDataSourceList(List<CustomDataSource> customDataSources, Map<String, RemoteTable> customFileSources) {
            this.customDataSources = customDataSources;
            this.customFileSources = customFileSources;
        }
    }

    public static class ThirdPartyCustomDatabase {
        public final List<Parameter> parameters;
        public final String type;

        public ThirdPartyCustomDatabase(String type, List<Parameter> parameters) {
            this.parameters = parameters;
            this.type = type;
        }
    }

    public static class DiscoverableRemoteTable {
        public final URL url;
        public final boolean indexUrl;
        public final List<SchemaField> columns;
        public final org.rakam.analysis.datasource.RemoteTable.CompressionType compressionType;
        public final org.rakam.analysis.datasource.RemoteTable.ExternalSourceType format;
        public final Map<String, String> typeOptions;

        @JsonCreator
        public DiscoverableRemoteTable(
                @ApiParam(value = "url") URL url,
                @ApiParam(value = "indexUrl", required = false) Boolean indexUrl,
                @ApiParam(value = "typeOptions", required = false) Map<String, String> typeOptions,
                @ApiParam(value = "columns", required = false) List<SchemaField> columns,
                @ApiParam(value = "compressionType", required = false) RemoteTable.CompressionType compressionType,
                @ApiParam(value = "format") org.rakam.analysis.datasource.RemoteTable.ExternalSourceType format) {
            this.url = url;
            this.indexUrl = indexUrl == Boolean.TRUE;
            this.typeOptions = Optional.ofNullable(typeOptions).orElse(ImmutableMap.of());
            this.columns = columns;
            this.compressionType = compressionType;
            this.format = requireNonNull(format, "format is null");
        }

        public RemoteTable getTable() {
            List<SchemaField> columns = (this.columns == null ? ExternalFileCustomDataSource.fillColumnIfNotSet(typeOptions, format, url, indexUrl) : this.columns);

            return new RemoteTable(url,
                    indexUrl, typeOptions,
                    columns,
                    compressionType, format);
        }
    }
}
