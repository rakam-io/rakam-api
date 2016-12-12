package org.rakam.analysis.datasource;

import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
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
import org.rakam.util.JsonHelper;
import org.rakam.util.SuccessMessage;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;

@IgnoreApi
@Path("/custom-data-source")
@Api(value = "/custom-data-source", nickname = "custom-data-source", description = "Connect to custom databases", tags = {"analyze"})
public class CustomDataSourceHttpService
        extends HttpService
{
    private final CustomDataSourceService service;

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

    @Inject
    public CustomDataSourceHttpService(CustomDataSourceService service)
    {
        this.service = service;
    }

    @ApiOperation(value = "List data-sources", authorizations = @Authorization(value = "master_key"))
    @Path("/list")
    @GET
    public CustomDataSourceService.CustomDataSourceList listDatabases(@Named("project") String project)
    {
        CustomDataSourceService.CustomDataSourceList customDataSourceList = service.listDatabases(project);
        for (CustomDataSource customDataSource : customDataSourceList.customDataSources) {
            customDataSource.options.setPassword(null);
        }
        return customDataSourceList;
    }

    @ApiOperation(value = "Schema of data-sources", authorizations = @Authorization(value = "master_key"))
    @Path("/schema")
    @GET
    public Map<String, Map<String, List<SchemaField>>> schemaDatabases(@Named("project") String project)
    {
        return service.schemaDatabases(project);
    }

    @ApiOperation(value = "Get data-source", authorizations = @Authorization(value = "master_key"))
    @Path("/get/database")
    @JsonRequest
    public CustomDataSource getDatabase(@Named("project") String project, String schema)
    {
        return service.getDatabase(project, schema);
    }

    @ApiOperation(value = "Get data-source", authorizations = @Authorization(value = "master_key"))
    @Path("/get/file")
    @JsonRequest
    public RemoteTable getFile(@Named("project") String project, String tableName)
    {
        return service.getFile(project, tableName);
    }

    @ApiOperation(value = "Get file", authorizations = @Authorization(value = "master_key"))
    @Path("/list/file")
    @JsonRequest
    public Map<String, RemoteTable> getFiles(@Named("project") String project)
    {
        return service.getFiles(project);
    }

    @ApiOperation(value = "Add data-source", authorizations = @Authorization(value = "master_key"))
    @Path("/add/database")
    @JsonRequest
    public SuccessMessage addDatabase(@Named("project") String project, @BodyParam CustomDataSource hook)
    {
        return service.addDatabase(project, hook);
    }

    @ApiOperation(value = "Add file data-source", authorizations = @Authorization(value = "master_key"))
    @Path("/add/file")
    @JsonRequest
    public SuccessMessage addFile(@Named("project") String project, @ApiParam("tableName") String tableName, @ApiParam("options") CustomDataSourceService.DiscoverableRemoteTable hook)
    {
        return service.addFile(project, tableName, hook);
    }

    @ApiOperation(value = "Add file data-source", authorizations = @Authorization(value = "master_key"))
    @Path("/remove/file")
    @JsonRequest
    public SuccessMessage removeFile(@Named("project") String project, @ApiParam("tableName") String tableName)
    {
        return service.removeFile(project, tableName);
    }

    @ApiOperation(value = "Add file data-source", authorizations = @Authorization(value = "master_key"))
    @Path("/remove/database")
    @JsonRequest
    public SuccessMessage removeDatabase(@Named("project") String project, @ApiParam("schemaName") String schemaName)
    {
        return service.removeDatabase(project, schemaName);
    }

    @ApiOperation(value = "Test database", authorizations = @Authorization(value = "master_key"))
    @Path("/test/database")
    @JsonRequest
    public SuccessMessage testDatabase(@Named("project") String project, @ApiParam("type") String type, @ApiParam(value = "options") JDBCSchemaConfig options)
    {
        return service.testDatabase(project, type, options);
    }

    @ApiOperation(value = "Test database", authorizations = @Authorization(value = "master_key"))
    @Path("/test/file")
    @JsonRequest
    public SuccessMessage testFile(@Named("project") String project, @BodyParam CustomDataSourceService.DiscoverableRemoteTable hook)
    {
        return service.testFile(project, hook);
    }
}
