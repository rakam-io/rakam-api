package org.rakam.analysis.datasource;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.AlreadyExistsException;
import org.rakam.util.JDBCUtil;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;
import org.rakam.util.SuccessMessage;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.Path;

import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static java.util.Objects.requireNonNull;
import static org.rakam.util.JDBCUtil.fromSql;

public class CustomDataSourceService
{
    private final DBI dbi;

    @Inject
    public CustomDataSourceService(@Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource)
    {
        this.dbi = new DBI(dataSource);
    }

    @PostConstruct
    public void setup()
    {
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

    public static class CustomDataSourceList
    {
        public final List<CustomDataSource> customDataSources;
        public final Map<String, RemoteTable> customFileSources;

        @JsonCreator
        public CustomDataSourceList(List<CustomDataSource> customDataSources, Map<String, RemoteTable> customFileSources)
        {
            this.customDataSources = customDataSources;
            this.customFileSources = customFileSources;
        }
    }

    public CustomDataSourceList listDatabases(@Named("project") String project)
    {
        try (Handle handle = dbi.open()) {
            List<CustomDataSource> customDataSources = handle.createQuery("SELECT schema_name, type, options FROM custom_data_source WHERE project = :project")
                    .bind("project", project)
                    .map((index, r, ctx) -> {
                        JDBCSchemaConfig read = JsonHelper.read(r.getString(3), JDBCSchemaConfig.class);
                        read.setPassword(null);
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

    public Map<String, Map<String, List<SchemaField>>> schemaDatabases(@Named("project") String project)
    {
        ImmutableMap.Builder<String, Map<String, List<SchemaField>>> schemas = ImmutableMap.builder();

        CustomDataSourceList customDataSourceList = listDatabases(project);
        for (CustomDataSource customDataSource : customDataSourceList.customDataSources) {
            Map<String, List<SchemaField>> builder = new HashMap<>();

            SupportedCustomDatabase source = SupportedCustomDatabase.getAdapter(customDataSource.type);
            JDBCSchemaConfig convert = JsonHelper.convert(customDataSource.options, JDBCSchemaConfig.class);

            try (Connection conn = source.getTestFunction().openConnection(convert)) {
                ResultSet dbColumns = conn.getMetaData().getColumns(null, convert.getSchema(), null, null);

                int i = 0;
                while (dbColumns.next()) {
                    String columnName = dbColumns.getString("COLUMN_NAME");
                    FieldType fieldType;
                    fieldType = fromSql(dbColumns.getInt("DATA_TYPE"), dbColumns.getString("TYPE_NAME"), JDBCUtil::getType);
                    builder.computeIfAbsent(dbColumns.getString("table_name"), (k) -> new ArrayList<>())
                            .add(new SchemaField(columnName, fieldType));
                    if (i++ > 20) {
                        break;
                    }
                }
            }
            catch (SQLException e) {
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
    }

    public CustomDataSource getDatabase(@Named("project") String project, String schema)
    {
        try (Handle handle = dbi.open()) {
            Query<Map<String, Object>> bind = handle.createQuery("SELECT type, options FROM custom_data_source WHERE project = :project AND schema_name = :schema_name")
                    .bind("project", project)
                    .bind("schema_name", schema);

            CustomDataSource first = bind.map((index, r, ctx) -> {
                return new CustomDataSource(r.getString(1), schema, JsonHelper.read(r.getString(2), org.rakam.analysis.datasource.JDBCSchemaConfig.class));
            }).first();

            if (first == null) {
                throw new RakamException(NOT_FOUND);
            }

            return first;
        }
    }

    public RemoteTable getFile(@Named("project") String project, String tableName)
    {
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

    public Map<String, RemoteTable> getFiles(@Named("project") String project)
    {
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

    public SuccessMessage addDatabase(@Named("project") String project, CustomDataSource hook)
    {
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
            }
            catch (Exception e) {
                try {
                    getDatabase(project, hook.schemaName);
                    throw new AlreadyExistsException("Custom database", BAD_REQUEST);
                }
                catch (RakamException e1) {
                    if (e1.getStatusCode() != NOT_FOUND) {
                        throw e1;
                    }
                    else {
                        throw e;
                    }
                }
            }
        }
    }

    @ApiOperation(value = "Add file data-source", authorizations = @Authorization(value = "master_key"))
    @Path("/add/file")
    @JsonRequest
    public SuccessMessage addFile(@Named("project") String project, @ApiParam("tableName") String tableName, @ApiParam("options") DiscoverableRemoteTable hook)
    {
        try (Handle handle = dbi.open()) {
            try {
                handle.createStatement("INSERT INTO custom_file_source (project, table_name, options) " +
                        "VALUES (:project, :table_name, :options)")
                        .bind("project", project)
                        .bind("table_name", tableName)
                        .bind("options", JsonHelper.encode(hook.getTable()))
                        .execute();
                return SuccessMessage.success();
            }
            catch (Exception e) {
                try {
                    getFile(project, tableName);
                    throw new AlreadyExistsException("Custom file", BAD_REQUEST);
                }
                catch (RakamException e1) {
                    if (e1.getStatusCode() != NOT_FOUND) {
                        throw e1;
                    }
                    else {
                        throw e;
                    }
                }
            }
        }
    }

    @ApiOperation(value = "Add file data-source", authorizations = @Authorization(value = "master_key"))
    @Path("/remove/file")
    @JsonRequest
    public SuccessMessage removeFile(@Named("project") String project, @ApiParam("tableName") String tableName)
    {
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

    @ApiOperation(value = "Add file data-source", authorizations = @Authorization(value = "master_key"))
    @Path("/remove/database")
    @JsonRequest
    public SuccessMessage removeDatabase(@Named("project") String project, @ApiParam("schemaName") String schemaName)
    {
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

    @ApiOperation(value = "Test database", authorizations = @Authorization(value = "master_key"))
    @Path("/test/database")
    @JsonRequest
    public SuccessMessage testDatabase(@Named("project") String project, String type, JDBCSchemaConfig options)
    {
        SupportedCustomDatabase optionalFunction = SupportedCustomDatabase.getAdapter(type);
        Optional<String> test = optionalFunction.getTestFunction().test(options);
        if (test.isPresent()) {
            throw new RakamException(test.get(), BAD_REQUEST);
        }

        return SuccessMessage.success();
    }

    @ApiOperation(value = "Test database", authorizations = @Authorization(value = "master_key"))
    @Path("/test/file")
    @JsonRequest
    public SuccessMessage testFile(@Named("project") String project, DiscoverableRemoteTable hook)
    {
        ExternalFileCustomDataSource source = new ExternalFileCustomDataSource();
        Optional<String> test = source.test(hook.getTable());
        if (test.isPresent()) {
            throw new RakamException(test.get(), BAD_REQUEST);
        }

        return SuccessMessage.success();
    }

    public static class DiscoverableRemoteTable
    {
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
                @ApiParam(value = "format") org.rakam.analysis.datasource.RemoteTable.ExternalSourceType format)
        {
            this.url = url;
            this.indexUrl = indexUrl == Boolean.TRUE;
            this.typeOptions = Optional.ofNullable(typeOptions).orElse(ImmutableMap.of());
            this.columns = columns;
            this.compressionType = compressionType;
            this.format = requireNonNull(format, "format is null");
        }

        public RemoteTable getTable()
        {
            List<SchemaField> columns = (this.columns == null ? ExternalFileCustomDataSource.fillColumnIfNotSet(typeOptions, format, url, indexUrl) : this.columns);

            return new RemoteTable(url,
                    indexUrl, typeOptions,
                    columns,
                    compressionType, format);
        }
    }
}
