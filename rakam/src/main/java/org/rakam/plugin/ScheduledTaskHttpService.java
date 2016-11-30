package org.rakam.plugin;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.collection.util.JSCodeCompiler;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.BodyParam;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;
import org.rakam.util.SuccessMessage;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;
import javax.script.Invocable;
import javax.script.ScriptException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.util.concurrent.TimeUnit.MINUTES;

@Path("/scheduled-task")
@Api(value = "/scheduled-task", nickname = "task", description = "Tasks for automatic event collection", tags = {"collect", "task"})
public class ScheduledTaskHttpService
        extends HttpService
{
    private final DBI dbi;
    private final ScheduledExecutorService executor;
    private final JSCodeCompiler jsCodeCompiler;

    @Inject
    public ScheduledTaskHttpService(@Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource, JSCodeCompiler jsCodeCompiler)
    {
        this.dbi = new DBI(dataSource);
        this.executor = Executors.newScheduledThreadPool(1);
        this.jsCodeCompiler = jsCodeCompiler;
    }

    @PostConstruct
    public void schedule()
    {
        executor.scheduleWithFixedDelay(() -> {
            try (Handle handle = dbi.open()) {
                List<ScheduledTask> tasks = handle.createQuery("SELECT id, code, parameters FROM custom_scheduled_tasks WHERE updated_at + schedule_interval > now() FOR UPDATE")
                        .map((index, r, ctx) -> {
                            return new ScheduledTask(r.getInt(1), r.getString(2), JsonHelper.read(r.getString(2)), null, null);
                        }).list();
                for (ScheduledTask task : tasks) {
                    try {
                        Invocable engine = jsCodeCompiler.createEngine(task.code);
                        Object main = engine.invokeFunction("main", task.parameters);
                        if (main instanceof List) {
                            System.out.println(1);
                        }
                    }
                    catch (ScriptException e) {
                        e.printStackTrace();
                    }
                    catch (NoSuchMethodException e) {
                        e.printStackTrace();
                    }
                }
            }
        }, 0, 5, MINUTES);
    }

    @PostConstruct
    public void setup()
    {
        try (Handle handle = dbi.open()) {
            handle.createStatement("CREATE TABLE IF NOT EXISTS custom_scheduled_tasks (" +
                    "  id SERIAL PRIMARY KEY," +
                    "  project VARCHAR(255) NOT NULL," +
                    "  code TEXT NOT NULL," +
                    "  parameters TEXT," +
                    "  updated_at TIMESTAMP," +
                    "  schedule_interval INT" +
                    "  )")
                    .execute();
        }
    }

    @GET
    @ApiOperation(value = "List tasks", authorizations = @Authorization(value = "master_key"))
    @Path("/list")
    public List<ScheduledTask> list(@Named("project") String project)
    {
        try (Handle handle = dbi.open()) {
            return handle.createQuery("SELECT id, code, parameters, updated_at FROM custom_scheduled_tasks WHERE project = :project")
                    .bind("project", project).map((index, r, ctx) -> {
                        return new ScheduledTask(r.getInt(1), r.getString(2), JsonHelper.read(r.getString(3)), Duration.ofSeconds(r.getInt(5)), r.getTimestamp(4).toInstant());
                    }).list();
        }
    }

    @JsonRequest
    @ApiOperation(value = "Create task", authorizations = @Authorization(value = "master_key"))
    @Path("/create")
    public SuccessMessage create(@Named("project") String project, @ApiParam("code") String code, @ApiParam("parameters") Map<String, Object> parameters, @ApiParam("interval") Duration interval)
    {
        try (Handle handle = dbi.open()) {
            handle.createStatement("INSERT INTO custom_scheduled_tasks (project, code, parameters, schedule_interval, updated_at) VALUES (:project, :code, :interval, :parameters, 0)")
                    .bind("project", project)
                    .bind("code", code)
                    .bind("interval", interval.getSeconds())
                    .bind("parameters", JsonHelper.encode(parameters))
                    .execute();
            return SuccessMessage.success();
        }
    }

    @JsonRequest
    @ApiOperation(value = "Delete task", authorizations = @Authorization(value = "master_key"))
    @Path("/delete")
    public SuccessMessage delete(@Named("project") String project, @ApiParam("id") int id)
    {
        try (Handle handle = dbi.open()) {
            handle.createStatement("DELETE FROM custom_scheduled_tasks WHERE project = :project AND id = :id")
                    .bind("project", project)
                    .bind("code", id)
                    .execute();
            return SuccessMessage.success();
        }
    }

    @JsonRequest
    @ApiOperation(value = "Trigger task", authorizations = @Authorization(value = "master_key"))
    @Path("/trigger")
    public SuccessMessage trigger(@Named("project") String project, @ApiParam("id") int id)
    {
        try (Handle handle = dbi.open()) {
            handle.createStatement("DELETE FROM custom_scheduled_tasks WHERE project = :project AND id = :id")
                    .bind("project", project)
                    .bind("code", id)
                    .execute();
            return SuccessMessage.success();
        }
    }

    @JsonRequest
    @ApiOperation(value = "Update task", authorizations = @Authorization(value = "master_key"))
    @Path("/update")
    public SuccessMessage update(@Named("project") String project, @BodyParam ScheduledTask mapper)
    {
        try (Handle handle = dbi.open()) {
            handle.createStatement("UPDATE custom_scheduled_tasks SET code = :code, parameters = :parameters, schedule_interval = :interval WHERE id = :id AND project = :project")
                    .bind("project", project)
                    .bind("id", mapper.id)
                    .bind("interval", mapper.interval)
                    .bind("parameters", mapper.parameters)
                    .bind("code", mapper.code);
            return SuccessMessage.success();
        }
    }

    @JsonRequest
    @ApiOperation(value = "Test task", authorizations = @Authorization(value = "master_key"))
    @Path("/test")
    public CompletableFuture<Object> test(@Named("project") String project, @ApiParam(value = "script") String script, @ApiParam(value = "parameters", required = false) Map<String, Object> parameters)
    {
        return CompletableFuture.supplyAsync(() -> {
            try {
                Invocable engine = jsCodeCompiler.createEngine(script);

                Object mapper = engine.invokeFunction("main", parameters);

                if (mapper == null) {
                    return null;
                }

                if (mapper instanceof List) {
                    for (Object elem : ((List) mapper)) {
                        if (elem instanceof Map) {
                            Object collection = ((Map) elem).get("collection");
                            Object properties = ((Map) elem).get("properties");
                        }
                        else {
                            continue;
                        }
                    }
                }
                else {
                    throw new RakamException("The script didn't return a list of events, instead: " +
                            JsonHelper.encode(mapper),
                            BAD_REQUEST);
                }

                return mapper;
            }
            catch (ScriptException e) {
                throw new RakamException("Error executing script: " + e.getMessage(), BAD_REQUEST);
            }
            catch (NoSuchMethodException e) {
                throw new RakamException("There must be a function called 'main'.", BAD_REQUEST);
            }
        }, executor);
    }

    public static class ScheduledTask
    {
        public final int id;
        public final String code;
        public final Map<String, String> parameters;
        public final Instant lastUpdated;
        public final Duration interval;

        @JsonCreator
        public ScheduledTask(
                @ApiParam("id") int id,
                @ApiParam("code") String code,
                @ApiParam("parameters") Map<String, String> parameters,
                @ApiParam("interval") Duration interval,
                @ApiParam("updated_at") Instant lastUpdated)
        {
            this.id = id;
            this.code = code;
            this.parameters = parameters;
            this.interval = interval;
            this.lastUpdated = lastUpdated;
        }
    }
}
