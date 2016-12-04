package org.rakam.ui;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.inject.name.Named;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.collection.FieldType;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.util.JsonHelper;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import java.util.List;
import java.util.Map;

@IgnoreApi
@Path("/ui/scheduled-task")
@Api(value = "/ui/scheduled-task")
public class ScheduledTaskUIHttpService
        extends HttpService
{
    private final DBI dbi;

    @Inject
    public ScheduledTaskUIHttpService(@Named("ui.metadata.jdbc") JDBCPoolDataSource dataSource)
    {
        this.dbi = new DBI(dataSource);
    }

    @PostConstruct
    public void setup()
    {
        try (Handle handle = dbi.open()) {
            handle.createStatement("CREATE TABLE IF NOT EXISTS predefined_scheduled_task (" +
                    "  name VARCHAR(255) NOT NULL," +
                    "  image TEXT NOT NULL," +
                    "  description TEXT NOT NULL," +
                    "  code TEXT," +
                    "  parameters TEXT," +
                    "  default_interval TEXT," +
                    "  PRIMARY KEY (name)" +
                    "  )")
                    .execute();
        }
    }

    @GET
    @ApiOperation(value = "List scheduled job", response = Integer.class)
    @Path("/list")
    public List<ScheduledTask> list()
    {
        try (Handle handle = dbi.open()) {
            return handle.createQuery("SELECT name, image, description, code, parameters FROM predefined_scheduled_task")
                    .map((index, r, ctx) -> {
                        return new ScheduledTask(r.getString(1), r.getString(2), r.getString(3), r.getString(4),
                                JsonHelper.read(r.getString(5), Map.class));
                    }).list();
        }
    }

    public static class Parameter
    {
        public final FieldType type;
        public final Object value;
        public final String placeholder;
        public final String description;

        @JsonCreator
        public Parameter(
                @ApiParam("type") FieldType type,
                @ApiParam(value = "value", required = false) Object value,
                @ApiParam("placeholder") String placeholder,
                @ApiParam("description") String description)
        {
            this.type = type;
            this.value = value;
            this.placeholder = placeholder;
            this.description = description;
        }
    }

    public static class ScheduledTask
    {
        public final String name;
        public final String image;
        public final String description;
        public final String code;
        public final Map<String, Parameter> parameters;

        @JsonCreator
        public ScheduledTask(@ApiParam("name") String name,
                @ApiParam("image") String image,
                @ApiParam("description") String description,
                @ApiParam("code") String code,
                @ApiParam("parameters") Map<String, Parameter> parameters)
        {
            this.name = name;
            this.image = image;
            this.description = description;
            this.code = code;
            this.parameters = parameters;
        }
    }
}
