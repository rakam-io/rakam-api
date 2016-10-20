package org.rakam.presto.analysis.datasource;

import com.facebook.presto.rakam.externaldata.DataManager;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.presto.analysis.datasource.CustomDataSource.ExternalFileCustomDataSource;
import org.rakam.presto.analysis.datasource.CustomDataSource.SupportedCustomDatabase;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.BodyParam;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.server.http.annotations.JsonRequest;
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

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.rakam.presto.analysis.datasource.CustomDataSource.DataSource.createDataSource;

@IgnoreApi
@Path("/custom-data-source")
@Api(value = "/integrate/data-source", nickname = "custom-data-source", description = "Connect to custom databases", tags = {"analyze"})
public class CustomDataSourceHttpService
        extends HttpService
{
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

    @ApiOperation(value = "List data-sources", authorizations = @Authorization(value = "master_key"))
    @Path("/list")
    @GET
    public List<CustomDataSource> list(@Named("project") String project)
    {
        try (Handle handle = dbi.open()) {
            return handle.createQuery("SELECT schema_name, type, options FROM custom_data_source WHERE project = :project")
                    .bind("project", project)
                    .map((index, r, ctx) -> {
                        DataManager.DataSourceFactory dataSource = createDataSource(r.getString(2), JsonHelper.read(r.getString(3)));
                        return new CustomDataSource(r.getString(2), r.getString(1), dataSource);
                    }).list();
        }
    }

    @ApiOperation(value = "Get data-source", authorizations = @Authorization(value = "master_key"))
    @Path("/get")
    @JsonRequest
    public CustomDataSource get(@Named("project") String project, String schema)
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

    @ApiOperation(value = "Add data-source", authorizations = @Authorization(value = "master_key"))
    @Path("/add/database")
    @JsonRequest
    public SuccessMessage add(@Named("project") String project, @BodyParam CustomDataSource hook)
    {
        try (Handle handle = dbi.open()) {
            try {
                handle.createStatement("INSERT INTO custom_data_source (project, schema_name, type, options) " +
                        "VALUES (:project, :schema_name, :table_name, :type, :options)")
                        .bind("project", project)
                        .bind("schema_name", hook.schemaName)
                        .bind("type", hook.type)
                        .bind("options", JsonHelper.encode(hook.options))
                        .execute();
                return SuccessMessage.success();
            }
            catch (Exception e) {
                throw e;
//                throw new RakamException(e.getMessage(), BAD_REQUEST);
            }
        }
    }

    @ApiOperation(value = "Add file data-source", authorizations = @Authorization(value = "master_key"))
    @Path("/add/file")
    @JsonRequest
    public SuccessMessage add(@Named("project") String project, @BodyParam CustomFile hook)
    {
        try (Handle handle = dbi.open()) {
            try {
                handle.createStatement("INSERT INTO custom_file_source (project, table_name, type, options) " +
                        "VALUES (:project, :schema_name, :table_name, :type, :options)")
                        .bind("project", project)
                        .bind("table_name", hook.tableName)
                        .bind("options", JsonHelper.encode(hook.options))
                        .execute();
                return SuccessMessage.success();
            }
            catch (Exception e) {
                throw e;
//                throw new RakamException(e.getMessage(), BAD_REQUEST);
            }
        }
    }

    @ApiOperation(value = "Test data-source", authorizations = @Authorization(value = "master_key"))
    @Path("/test")
    @JsonRequest
    public SuccessMessage test(@Named("project") String project, @BodyParam CustomDataSource hook)
    {
        Function optionalFunction = SupportedCustomDatabase.getTestFunction(hook.type);
        Optional<String> test = (Optional<String>) optionalFunction.apply(hook.options);
        if (test.isPresent()) {
            throw new RakamException(test.get(), BAD_REQUEST);
        }

        return SuccessMessage.success();
    }
}
