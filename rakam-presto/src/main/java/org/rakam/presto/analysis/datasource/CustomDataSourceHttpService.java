package org.rakam.presto.analysis.datasource;

import com.facebook.presto.jdbc.internal.guava.base.Function;
import com.facebook.presto.rakam.externaldata.DataManager;
import com.facebook.presto.rakam.externaldata.source.RemoteFileDataSource;
import com.facebook.presto.rakam.externaldata.source.RemoteFileDataSource.Column;
import com.facebook.presto.rakam.externaldata.source.RemoteFileDataSource.CompressionType;
import com.facebook.presto.rakam.externaldata.source.RemoteFileDataSource.ExternalSourceType;
import com.facebook.presto.rakam.externaldata.source.RemoteFileDataSource.RemoteTable;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableMap;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.collection.SchemaField;
import org.rakam.presto.analysis.PrestoMetastore;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.BodyParam;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.server.http.annotations.JsonRequest;
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
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static java.util.Objects.requireNonNull;
import static org.rakam.presto.analysis.datasource.DataSource.createDataSource;

@IgnoreApi
@Path("/custom-data-source")
@Api(value = "/integrate/data-source", nickname = "custom-data-source", description = "Connect to custom databases", tags = {"analyze"})
public class CustomDataSourceHttpService
        extends HttpService
{
    static {
        JsonHelper.getMapper().registerModule(new SimpleModule()
                .addDeserializer(Type.class, new JsonDeserializer<Type>()
                {
                    @Override
                    public Type deserialize(JsonParser jp, DeserializationContext ctxt)
                            throws IOException
                    {
                        return new PrestoMetastore.SignatureReferenceType(parseTypeSignature(jp.getValueAsString()), null);
                    }
                }).addSerializer(Type.class, new JsonSerializer<Type>()
                {
                    @Override
                    public void serialize(Type value, JsonGenerator jgen, SerializerProvider provider)
                            throws IOException
                    {
                        jgen.writeString(value.getTypeSignature().toString());
                    }
                }));
    }

    private final DBI dbi;

    @Inject
    public CustomDataSourceHttpService(@Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource)
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

    @ApiOperation(value = "Delete hook", authorizations = @Authorization(value = "master_key"))
    @Path("/delete")
    @JsonRequest
    public void delete(@Named("project") String project, @ApiParam("identifier") String identifier)
    {
        try (Handle handle = dbi.open()) {
            handle.createQuery("SELECT options FROM custom_data_source WHERE project = :project")
                    .bind("project", project)
                    .map((index, r, ctx) -> {
                        return JsonHelper.read(r.getString(1), ExternalFileCustomDataSource.class);
                    }).list();
        }
    }

    public static class CustomDataSourceList
    {
        public final List<CustomDataSource> customDataSources;
        public final List<RemoteTable> customFileSources;

        @JsonCreator
        public CustomDataSourceList(List<CustomDataSource> customDataSources, List<RemoteTable> customFileSources)
        {
            this.customDataSources = customDataSources;
            this.customFileSources = customFileSources;
        }
    }

    @ApiOperation(value = "List data-sources", authorizations = @Authorization(value = "master_key"))
    @Path("/list")
    @GET
    public CustomDataSourceList listDatabases(@Named("project") String project)
    {
        try (Handle handle = dbi.open()) {
            List<CustomDataSource> customDataSources = handle.createQuery("SELECT schema_name, type, options FROM custom_data_source WHERE project = :project")
                    .bind("project", project)
                    .map((index, r, ctx) -> {
                        DataManager.DataSourceFactory dataSource = createDataSource(r.getString(2), JsonHelper.read(r.getString(3)));
                        return new CustomDataSource(r.getString(2), r.getString(1), dataSource);
                    }).list();

            List<RemoteTable> remoteTables = handle.createQuery("SELECT options FROM custom_file_source WHERE project = :project")
                    .bind("project", project)
                    .map((index, r, ctx) -> {
                        return JsonHelper.read(r.getString(1), RemoteTable.class);
                    }).list();

            return new CustomDataSourceList(customDataSources, remoteTables);
        }
    }

    @ApiOperation(value = "Get data-source", authorizations = @Authorization(value = "master_key"))
    @Path("/get/database")
    @JsonRequest
    public CustomDataSource getDatabase(@Named("project") String project, String schema)
    {
        try (Handle handle = dbi.open()) {
            Query<Map<String, Object>> bind = handle.createQuery("SELECT type, options FROM custom_data_source WHERE project = :project AND schema_name = :schema_name")
                    .bind("project", project)
                    .bind("schema_name", schema);

            CustomDataSource first = bind.map((index, r, ctx) -> {
                DataManager.DataSourceFactory dataSource = createDataSource(r.getString(1), JsonHelper.read(r.getString(2)));
                return new CustomDataSource(r.getString(1), r.getString(2), dataSource);
            }).first();

            if (first == null) {
                throw new RakamException(NOT_FOUND);
            }

            return first;
        }
    }

    @ApiOperation(value = "Get data-source", authorizations = @Authorization(value = "master_key"))
    @Path("/get/file")
    @JsonRequest
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

    @ApiOperation(value = "Get file", authorizations = @Authorization(value = "master_key"))
    @Path("/list/file")
    @JsonRequest
    public List<RemoteTable> getFiles(@Named("project") String project)
    {
        try (Handle handle = dbi.open()) {
            Query<Map<String, Object>> bind = handle.createQuery("SELECT options FROM custom_file_source WHERE project = :project")
                    .bind("project", project);

            return bind.map((index, r, ctx) -> {
                return JsonHelper.read(r.getString(1), RemoteFileDataSource.RemoteTable.class);
            }).list();
        }
    }

    @ApiOperation(value = "Add data-source", authorizations = @Authorization(value = "master_key"))
    @Path("/add/database")
    @JsonRequest
    public SuccessMessage addDatabase(@Named("project") String project, @BodyParam CustomDataSource hook)
    {
        try (Handle handle = dbi.open()) {
            try {
                handle.createStatement("INSERT INTO custom_data_source (project, schema_name, null, type, options) " +
                        "VALUES (:project, :schema_name, null, :type, :options)")
                        .bind("project", project)
                        .bind("schema_name", hook.schemaName)
                        .bind("type", hook.type)
                        .bind("table_name", (String) null)
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
                        .bind("options", JsonHelper.encode(hook.getTable(tableName)))
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

            if(execute == 0) {
                throw new RakamException("Custom file not found", NOT_FOUND);
            }

            return SuccessMessage.success();
        }
    }

    @ApiOperation(value = "Add file data-source", authorizations = @Authorization(value = "master_key"))
    @Path("/remove/database")
    @JsonRequest
    public SuccessMessage removeDatabase(@Named("project") String project, @ApiParam("tableName") String tableName)
    {
        try (Handle handle = dbi.open()) {
            int execute = handle.createStatement("DELETE FROM custom_data_source WHERE project = :project AND schema_name = :schema_name")
                    .bind("project", project)
                    .bind("schema_name", tableName)
                    .execute();

            if(execute == 0) {
                throw new RakamException("Custom database not found", NOT_FOUND);
            }

            return SuccessMessage.success();
        }
    }

    @ApiOperation(value = "Test database", authorizations = @Authorization(value = "master_key"))
    @Path("/test/database")
    @JsonRequest
    public SuccessMessage testDatabase(@Named("project") String project, @BodyParam CustomDataSource hook)
    {
        Function optionalFunction = SupportedCustomDatabase.getTestFunction(hook.type);
        Optional<String> test = (Optional<String>) optionalFunction.apply(hook.options);
        if (test.isPresent()) {
            throw new RakamException(test.get(), BAD_REQUEST);
        }

        return SuccessMessage.success();
    }

    @ApiOperation(value = "Test database", authorizations = @Authorization(value = "master_key"))
    @Path("/test/file")
    @JsonRequest
    public SuccessMessage testFile(@Named("project") String project, @BodyParam DiscoverableRemoteTable hook)
    {
        ExternalFileCustomDataSource source = new ExternalFileCustomDataSource();
        Optional<String> test = source.test(hook.getTable("dummy"));
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
        public final CompressionType compressionType;
        public final ExternalSourceType format;
        public final Map<String, String> typeOptions;

        @JsonCreator
        public DiscoverableRemoteTable(
                @JsonProperty("url") URL url,
                @JsonProperty("indexUrl") Boolean indexUrl,
                @JsonProperty("typeOptions") Map<String, String> typeOptions,
                @JsonProperty("columns") List<SchemaField> columns,
                @JsonProperty("compressionType") CompressionType compressionType,
                @JsonProperty("format") ExternalSourceType format)
        {
            this.url = url;
            this.indexUrl = indexUrl == Boolean.TRUE;
            this.typeOptions = Optional.ofNullable(typeOptions).orElse(ImmutableMap.of());
            this.columns = columns;
            this.compressionType = compressionType;
            this.format = requireNonNull(format, "format is null");
        }

        public RemoteTable getTable(String name)
        {
            List<Column> columns = (this.columns == null ? ExternalFileCustomDataSource.fillColumnIfNotSet(typeOptions, format, url, indexUrl) : this.columns)
                    .stream().map(e -> {
                        TypeSignature signature = parseTypeSignature(PrestoMetastore.toSql(e.getType()));
                        return new Column(e.getName(), new PrestoMetastore.SignatureReferenceType(signature, null));
                    }).collect(Collectors.toList());

            return new RemoteTable(name, url,
                    indexUrl, typeOptions,
                    columns,
                    compressionType, format);
        }
    }
}
