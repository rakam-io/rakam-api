package org.rakam.analysis.datasource;

import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.rakam.analysis.RequestContext;
import org.rakam.collection.SchemaField;
import org.rakam.presto.analysis.PrestoRakamRaptorMetastore;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.*;
import org.rakam.util.JsonHelper;
import org.rakam.util.SuccessMessage;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;

@Path("/custom-data-source")
@Api(value = "/custom-data-source", nickname = "custom-data-source", description = "Connect to custom databases", tags = "analyze")
public class CustomDataSourceHttpService
        extends HttpService {
    static {
        JsonHelper.getMapper().registerModule(new SimpleModule()
                .addDeserializer(Type.class, new JsonDeserializer<Type>() {
                    @Override
                    public Type deserialize(JsonParser jp, DeserializationContext ctxt)
                            throws IOException {
                        return new PrestoRakamRaptorMetastore.SignatureReferenceType(parseTypeSignature(jp.getValueAsString()), null);
                    }
                }).addSerializer(Type.class, new JsonSerializer<Type>() {
                    @Override
                    public void serialize(Type value, JsonGenerator jgen, SerializerProvider provider)
                            throws IOException {
                        jgen.writeString(value.getTypeSignature().toString());
                    }
                }));
    }

    private final CustomDataSourceService service;

    @Inject
    public CustomDataSourceHttpService(CustomDataSourceService service) {
        this.service = service;
    }

    @ApiOperation(value = "List data-sources", authorizations = @Authorization(value = "master_key"))
    @Path("/list")
    @GET
    public CustomDataSourceService.CustomDataSourceList listDatabases(@Named("project") RequestContext context) {
        CustomDataSourceService.CustomDataSourceList customDataSourceList = service.listDatabases(context.project);
        for (CustomDataSource customDataSource : customDataSourceList.customDataSources) {
            customDataSource.options.setPassword(null);
        }
        return customDataSourceList;
    }

    @ApiOperation(value = "Schema of data-sources", authorizations = @Authorization(value = "read_key"))
    @Path("/schema/tables")
    @GET
    public CompletableFuture<Map<String, List<String>>> schemaDatabases(@Named("project") RequestContext context) {
        return service.schemaDatabases(context.project);
    }

    @ApiOperation(value = "Schema of table in data-sources", authorizations = @Authorization(value = "read_key"))
    @Path("/schema/table")
    @JsonRequest
    public CompletableFuture<List<SchemaField>> schemaDatabases(@Named("project") RequestContext context, @ApiParam("schema") String schema, @ApiParam("table") String table) {
        return service.schemaTable(context.project, schema, table);
    }

    @ApiOperation(value = "Get data-source", authorizations = @Authorization(value = "master_key"))
    @Path("/get/database")
    @JsonRequest
    public CustomDataSource getDatabase(@Named("project") RequestContext context, String schema) {
        return service.getDatabase(context.project, schema);
    }

    @ApiOperation(value = "Get data-source", authorizations = @Authorization(value = "master_key"))
    @Path("/get/file")
    @JsonRequest
    public RemoteTable getFile(@Named("project") RequestContext context, String tableName) {
        return service.getFile(context.project, tableName);
    }

    @ApiOperation(value = "Get file", authorizations = @Authorization(value = "master_key"))
    @Path("/list/file")
    @JsonRequest
    public Map<String, RemoteTable> getFiles(@Named("project") RequestContext context) {
        return service.getFiles(context.project);
    }

    @ApiOperation(value = "Add data-source", authorizations = @Authorization(value = "master_key"))
    @Path("/add/database")
    @JsonRequest
    public SuccessMessage addDatabase(@Named("project") RequestContext context, @BodyParam CustomDataSource hook) {
        return service.addDatabase(context.project, hook);
    }

    @ApiOperation(value = "Add file data-source", authorizations = @Authorization(value = "master_key"))
    @Path("/add/file")
    @JsonRequest
    public SuccessMessage addFile(@Named("project") RequestContext context, @ApiParam("tableName") String tableName, @ApiParam("options") CustomDataSourceService.DiscoverableRemoteTable hook) {
        return service.addFile(context.project, tableName, hook);
    }

    @ApiOperation(value = "Add file data-source", authorizations = @Authorization(value = "master_key"))
    @Path("/remove/file")
    @JsonRequest
    public SuccessMessage removeFile(@Named("project") RequestContext context, @ApiParam("tableName") String tableName) {
        return service.removeFile(context.project, tableName);
    }

    @ApiOperation(value = "Add file data-source", authorizations = @Authorization(value = "master_key"))
    @Path("/remove/database")
    @JsonRequest
    public SuccessMessage removeDatabase(@Named("project") RequestContext context, @ApiParam("schemaName") String schemaName) {
        return service.removeDatabase(context.project, schemaName);
    }

    @ApiOperation(value = "Test database", authorizations = @Authorization(value = "master_key"))
    @Path("/test/database")
    @JsonRequest
    public SuccessMessage testDatabase(@Named("project") RequestContext context, @ApiParam("type") String type, @ApiParam(value = "options") JDBCSchemaConfig options) {
        return service.testDatabase(context.project, type, options);
    }

    @ApiOperation(value = "Test database", authorizations = @Authorization(value = "master_key"))
    @Path("/test/file")
    @JsonRequest
    public SuccessMessage testFile(@Named("project") RequestContext context, @BodyParam CustomDataSourceService.DiscoverableRemoteTable hook) {
        return service.testFile(context.project, hook);
    }
}
