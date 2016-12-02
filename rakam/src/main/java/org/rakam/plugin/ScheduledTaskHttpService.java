package org.rakam.plugin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.cookie.Cookie;
import org.rakam.TestingConfigManager;
import org.rakam.analysis.ConfigManager;
import org.rakam.analysis.InMemoryApiKeyService;
import org.rakam.analysis.InMemoryMetastore;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.metadata.SchemaChecker;
import org.rakam.collection.Event;
import org.rakam.collection.Event.EventContext;
import org.rakam.collection.EventCollectionHttpService;
import org.rakam.collection.EventList;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.collection.JsonEventDeserializer;
import org.rakam.collection.util.JSCodeCompiler;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.BodyParam;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.ui.ScheduledTaskUIHttpService;
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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.rakam.plugin.EventMapper.RequestParams.EMPTY_PARAMS;
import static org.rakam.util.SuccessMessage.success;

@Path("/scheduled-task")
@Api(value = "/scheduled-task", nickname = "task", description = "Tasks for automatic event collection", tags = {"collect", "task"})
public class ScheduledTaskHttpService
        extends HttpService
{
    private final DBI dbi;
    private final ScheduledExecutorService executor;
    private final JSCodeCompiler jsCodeCompiler;
    private final JsonEventDeserializer eventDeserializer;
    private final FieldDependencyBuilder.FieldDependency fieldDependency;
    private final ConfigManager configManager;
    private final EventStore eventStore;
    private final InetAddress localhost;
    private final ImmutableList<EventMapper> eventMappers;

    @Inject
    public ScheduledTaskHttpService(
            @Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource,
            JsonEventDeserializer eventDeserializer,
            JSCodeCompiler jsCodeCompiler,
            ConfigManager configManager,
            Set<EventMapper> eventMapperSet,
            EventStore eventStore,
            FieldDependencyBuilder.FieldDependency fieldDependency)
    {
        this.dbi = new DBI(dataSource);
        this.executor = Executors.newScheduledThreadPool(1);
        this.jsCodeCompiler = jsCodeCompiler;
        this.eventMappers = ImmutableList.copyOf(eventMapperSet);
        this.eventDeserializer = eventDeserializer;
        this.eventStore = eventStore;
        this.configManager = configManager;
        this.fieldDependency = fieldDependency;
        try {
            localhost = InetAddress.getLocalHost();
        }
        catch (UnknownHostException e) {
            throw Throwables.propagate(e);
        }
    }

    @PostConstruct
    public void schedule()
    {
        executor.scheduleWithFixedDelay(() -> {
            try (Handle handle = dbi.open()) {
                List<ScheduledTask> tasks = handle.createQuery("SELECT id, project, name, code, parameters FROM custom_scheduled_tasks WHERE updated_at + schedule_interval > now() FOR UPDATE")
                        .map((index, r, ctx) -> {
                            return new ScheduledTask(r.getInt(1), r.getString(2), r.getString(3), r.getString(4), JsonHelper.read(r.getString(5)), null, null, null);
                        }).list();
                for (ScheduledTask task : tasks) {
                    String prefix = "scheduled-task." + task.id;
                    JSCodeCompiler.JavaLogger javaLogger = new JSCodeCompiler.JavaLogger(prefix);
                    JSCodeCompiler.JSConfigManager jsConfigManager = new JSCodeCompiler.JSConfigManager(configManager, task.project, prefix);
                    CompletableFuture<EventList> result = run(task.project, task.code, task.parameters, javaLogger, jsConfigManager, eventDeserializer);
                    result.whenComplete((events, ex) -> {
                        eventStore.storeBatchAsync(events.events).whenComplete((res, ex1) -> {

                        });
                    });
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
                    "  name VARCHAR(255) NOT NULL," +
                    "  image TEXT," +
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
            return handle.createQuery("SELECT id, project, name, code, parameters, image, schedule_interval, updated_at FROM custom_scheduled_tasks WHERE project = :project")
                    .bind("project", project).map((index, r, ctx) -> {
                        return new ScheduledTask(r.getInt(1), r.getString(2), r.getString(3), r.getString(4), JsonHelper.read(r.getString(5), new TypeReference<Map<String, ScheduledTaskUIHttpService.Parameter>>() {}), r.getString(6), Duration.ofSeconds(r.getInt(7)), r.getTimestamp(8).toInstant());
                    }).list();
        }
    }

    @JsonRequest
    @ApiOperation(value = "Create task", authorizations = @Authorization(value = "master_key"))
    @Path("/create")
    public SuccessMessage create(@Named("project") String project, @ApiParam("name") String name, @ApiParam("script") String code, @ApiParam("parameters") Map<String, ScheduledTaskUIHttpService.Parameter> parameters, @ApiParam("interval") Duration interval, @ApiParam(value = "image", required = false) String image)
    {
        try (Handle handle = dbi.open()) {
            handle.createStatement("INSERT INTO custom_scheduled_tasks (project, name, code, schedule_interval, parameters, updated_at, image) VALUES (:project, :name, :code, :interval, :parameters, :updated, :image)")
                    .bind("project", project)
                    .bind("name", name)
                    .bind("image", image)
                    .bind("code", code)
                    .bind("interval", interval.getSeconds())
                    .bind("parameters", JsonHelper.encode(parameters))
                    .bind("updated", Timestamp.from(Instant.ofEpochSecond(100)))
                    .execute();
            return success();
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
                    .bind("id", id)
                    .execute();
            return success();
        }
    }

    @JsonRequest
    @ApiOperation(value = "Trigger task", authorizations = @Authorization(value = "master_key"))
    @Path("/trigger")
    public CompletableFuture<SuccessMessage> trigger(@Named("project") String project, @ApiParam("id") int id)
    {
        try (Handle handle = dbi.open()) {
            Map<String, Object> first = handle.createQuery("SELECT code, parameters FROM custom_scheduled_tasks WHERE project = :project AND id = :id FOR UPDATE")
                    .bind("project", project)
                    .bind("id", id).first();

            String prefix = "scheduled-task." + id;

            JSCodeCompiler.JavaLogger javaLogger = new JSCodeCompiler.JavaLogger(prefix);
            JSCodeCompiler.JSConfigManager jsConfigManager = new JSCodeCompiler.JSConfigManager(configManager, project, prefix);

            CompletableFuture<EventList> future = run(project, first.get("code").toString(), JsonHelper.read(first.get("parameters").toString()), javaLogger, jsConfigManager, eventDeserializer);

            CompletableFuture<SuccessMessage> resultFuture = new CompletableFuture<>();

            future.whenComplete((events, ex) -> {
                if (ex != null) {
                    EventCollectionHttpService.mapEvent(eventMappers,
                            eventMapper -> eventMapper.mapAsync(events, EMPTY_PARAMS, localhost, HttpHeaders.EMPTY_HEADERS))
                            .whenComplete((value, ex1) -> {
                                if (ex1 == null) {
                                    eventStore.storeBatchAsync(events.events).whenComplete((failed, ex2) -> {
                                        if (ex2 == null) {
                                            handle.createStatement("UPDATE custom_scheduled_tasks SET updated_at = now() WHERE project = :project AND id = :id")
                                                    .bind("project", project)
                                                    .bind("id", id).execute();

                                            if (failed.length == 0) {
                                                resultFuture.complete(success(format("Processed %d events", events.events.size())));
                                            }
                                            else {
                                                resultFuture.complete(success(format("Processed %d events, %d of them failed", events.events.size(), failed.length)));
                                            }
                                        }
                                        else {
                                            resultFuture.completeExceptionally(ex2);
                                        }
                                    });
                                }
                                else {
                                    resultFuture.completeExceptionally(ex);
                                }
                            });
                }
                else {
                    resultFuture.completeExceptionally(ex);
                }
            });

            return resultFuture;
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
            return success();
        }
    }

    @JsonRequest
    @ApiOperation(value = "Test task", authorizations = @Authorization(value = "master_key"))
    @Path("/test")
    public CompletableFuture<Environment> test(@Named("project") String project, @ApiParam(value = "script") String script, @ApiParam(value = "parameters", required = false) Map<String, ScheduledTaskUIHttpService.Parameter> parameters)
    {
        JSCodeCompiler.TestLogger logger = new JSCodeCompiler.TestLogger();
        TestingConfigManager testingConfigManager = new TestingConfigManager();
        JSCodeCompiler.IJSConfigManager ijsConfigManager = new JSCodeCompiler.JSConfigManager(testingConfigManager, project, null);

        InMemoryApiKeyService apiKeyService = new InMemoryApiKeyService();
        InMemoryMetastore metastore = new InMemoryMetastore(apiKeyService);
        SchemaChecker schemaChecker = new SchemaChecker(metastore, new FieldDependencyBuilder().build());
        JsonEventDeserializer testingEventDeserializer = new JsonEventDeserializer(metastore,
                apiKeyService,
                testingConfigManager,
                schemaChecker,
                fieldDependency);
        metastore.createProject(project);

        return run(project, script, parameters, logger, ijsConfigManager, testingEventDeserializer).thenApply(eventList -> {
            if (eventList == null || eventList.events.isEmpty()) {
                logger.info("No event is returned");
            }
            else {
                logger.info(format("Successfully got %d events: %s: %s", eventList.events.size(), eventList.events.get(0).collection(), eventList.events.get(0).properties()));
            }

            return new Environment(logger.getEntries(), testingConfigManager.getTable().row(project));
        });
    }

    public static class Environment
    {
        public final List<JSCodeCompiler.TestLogger.LogEntry> logs;
        public final Map<String, Object> configs;

        public Environment(List<JSCodeCompiler.TestLogger.LogEntry> logs, Map<String, Object> configs)
        {
            this.logs = logs;
            this.configs = configs;
        }
    }

    private CompletableFuture<EventList> run(String project, String script, Map<String, ScheduledTaskUIHttpService.Parameter> parameters, JSCodeCompiler.ILogger logger, JSCodeCompiler.IJSConfigManager configManager, JsonEventDeserializer deserializer)
    {
        return CompletableFuture.supplyAsync(() -> {
            try {
                String code = script +
                        "\n var _main = main; \n" +
                        "main = function(parameters) { \n" +
                        "return JSON.stringify(_main(parameters)); \n" +
                        "}";

                Invocable engine = jsCodeCompiler.createEngine(code, logger, configManager);

                Map<String, Object> collect = Optional.ofNullable(parameters).map(v -> v.entrySet().stream()
                        .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().value)))
                        .orElse(ImmutableMap.of());

                Object mapper = engine.invokeFunction("main", collect);

                if (mapper == null) {
                    return null;
                }

                try {
                    JsonParser jp = JsonHelper.getMapper().getFactory().createParser(mapper.toString());
                    JsonToken t = jp.nextToken();

                    if (t != JsonToken.START_ARRAY) {
                        throw new RakamException("The script didn't return an array", BAD_REQUEST);
                    }

                    t = jp.nextToken();

                    List<Event> list = new ArrayList<>();
                    for (; t == START_OBJECT; t = jp.nextToken()) {
                        list.add(deserializer.deserializeWithProject(jp, project, EventContext.empty(), true));
                    }

                    return new EventList(EventContext.empty(), project, list);
                }
                catch (IOException e) {
                    throw new RakamException("Error parsing response: " + e.getMessage(), BAD_REQUEST);
                }
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
        public final Map<String, ScheduledTaskUIHttpService.Parameter> parameters;
        public final Instant lastUpdated;
        public final Duration interval;
        public final String project;
        public final String name;
        public final String image;

        @JsonCreator
        public ScheduledTask(
                @ApiParam("id") int id,
                @ApiParam("project") String project,
                @ApiParam("name") String name,
                @ApiParam("code") String code,
                @ApiParam("parameters") Map<String, ScheduledTaskUIHttpService.Parameter> parameters,
                @ApiParam("image") String image,
                @ApiParam("interval") Duration interval,
                @ApiParam("updated_at") Instant lastUpdated)
        {
            this.id = id;
            this.project = project;
            this.name = name;
            this.code = code;
            this.parameters = parameters;
            this.image = image;
            this.interval = interval;
            this.lastUpdated = lastUpdated;
        }
    }
}
