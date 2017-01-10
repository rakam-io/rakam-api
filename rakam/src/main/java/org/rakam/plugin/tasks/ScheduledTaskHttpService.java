package org.rakam.plugin.tasks;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.log.Logger;
import org.rakam.ServiceStarter;
import org.rakam.TestingConfigManager;
import org.rakam.analysis.ConfigManager;
import org.rakam.analysis.InMemoryApiKeyService;
import org.rakam.analysis.InMemoryEventStore;
import org.rakam.analysis.InMemoryMetastore;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.metadata.SchemaChecker;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.collection.JSCodeLoggerService;
import org.rakam.collection.JsonEventDeserializer;
import org.rakam.collection.util.JSCodeCompiler;
import org.rakam.collection.util.JSCodeCompiler.JSConfigManager;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.EventStore;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.BodyParam;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.ui.ScheduledTaskUIHttpService.Parameter;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;
import org.rakam.util.SuccessMessage;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.GeneratedKeys;
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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static java.lang.String.format;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.rakam.util.SuccessMessage.success;

@Path("/scheduled-task")
@Api(value = "/scheduled-task", nickname = "task", description = "Tasks for automatic event collection", tags = {"collect", "task"})
public class ScheduledTaskHttpService
        extends HttpService
{
    private final static Logger LOGGER = Logger.get(ServiceStarter.class);

    private final DBI dbi;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService executor;
    private final JSCodeCompiler jsCodeCompiler;
    private final JsonEventDeserializer eventDeserializer;
    private final FieldDependencyBuilder.FieldDependency fieldDependency;
    private final ConfigManager configManager;
    private final EventStore eventStore;
    private final LockService lockService;
    private final ImmutableList<EventMapper> eventMappers;
    private final String timestampToEpoch;
    private final JSCodeLoggerService service;

    @Inject
    public ScheduledTaskHttpService(
            @Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource,
            JsonEventDeserializer eventDeserializer,
            JSCodeCompiler jsCodeCompiler,
            LockService lockService,
            JSCodeLoggerService service,
            ConfigManager configManager,
            Set<EventMapper> eventMapperSet,
            @Named("timestamp_function") String timestampToEpoch,
            EventStore eventStore,
            FieldDependencyBuilder.FieldDependency fieldDependency)
    {
        this.dbi = new DBI(dataSource);
        this.service = service;
        this.scheduler = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder()
                .setNameFormat("scheduled-task-scheduler")
                .setUncaughtExceptionHandler((t, e) -> LOGGER.error(e))
                .build());
        this.executor = new ForkJoinPool
                (Runtime.getRuntime().availableProcessors(),
                        pool -> {
                            ForkJoinWorkerThread forkJoinWorkerThread = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                            forkJoinWorkerThread.setName("scheduled-task-worker");
                            return forkJoinWorkerThread;
                        },
                        null, true);
        this.jsCodeCompiler = jsCodeCompiler;
        this.eventMappers = ImmutableList.copyOf(eventMapperSet);
        this.eventDeserializer = eventDeserializer;
        this.eventStore = eventStore;
        this.configManager = configManager;
        this.timestampToEpoch = timestampToEpoch;
        this.fieldDependency = fieldDependency;
        this.lockService = lockService;
    }

    @PostConstruct
    public void schedule()
    {
        scheduler.scheduleAtFixedRate(() -> {
            try (Handle handle = dbi.open()) {
                List<Task> tasks = handle.createQuery(format("SELECT " +
                        "project, id, name, code, parameters FROM custom_scheduled_tasks " +
                        "WHERE last_executed_at is null or (last_executed_at + schedule_interval) < %s", timestampToEpoch))
                        .map((index, r, ctx) -> {
                            return new Task(r.getString(1), r.getInt(2), r.getString(3), r.getString(4), JsonHelper.read(r.getString(5), new TypeReference<Map<String, Parameter>>() {}));
                        }).list();
                for (Task task : tasks) {
                    LockService.Lock lock = lockService.tryLock(String.valueOf(task.id));

                    if (lock == null) {
                        continue;
                    }
                    long now = System.currentTimeMillis();
                    CompletableFuture<Void> run;
                    JSCodeLoggerService.PersistentLogger logger;
                    try {
                        String prefix = "scheduled-task." + task.id;
                        JSConfigManager jsConfigManager = new JSConfigManager(configManager, task.project, prefix);
                        logger = service.createLogger(task.project, prefix);

                        run = run(jsCodeCompiler, executor, task.project, task.script, task.parameters,
                                logger, jsConfigManager, eventDeserializer, eventStore, eventMappers);
                    }
                    catch (Throwable e) {
                        lock.release();
                        throw e;
                    }

                    run.whenComplete((events, ex) -> updateTask(task.project, task.id, lock, logger, now, ex));
                }
            }
            catch (Exception e) {
                LOGGER.error(e);
            }
        }, 0, 1, MINUTES);
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
                    "  last_executed_at BIGINT," +
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
            return handle.createQuery("SELECT id, name, code, parameters, image, schedule_interval, last_executed_at FROM custom_scheduled_tasks WHERE project = :project")
                    .bind("project", project).map((index, r, ctx) -> {
                        return new ScheduledTask(r.getInt(1), r.getString(2), r.getString(3), JsonHelper.read(r.getString(4), new TypeReference<Map<String, Parameter>>() {}), r.getString(5), Duration.ofSeconds(r.getInt(6)), Instant.ofEpochSecond(r.getLong(7)));
                    }).list();
        }
    }

    @ApiOperation(value = "List tasks", authorizations = @Authorization(value = "master_key"))
    @JsonRequest
    @Path("/get_logs")
    public List<JSCodeLoggerService.LogEntry> getLogs(@Named("project") String project, @ApiParam(value = "start", required = false) Instant start, @ApiParam(value = "end", required = false) Instant end, @ApiParam("id") int id)
    {
        LockService.Lock lock = null;
        boolean running;
        try {
            lock = lockService.tryLock(String.valueOf(id));
            running = lock == null;
        }
        finally {
            if (lock != null) {
                lock.release();
            }
        }

        return service.getLogs(project, start, end, "scheduled-task." + id);
    }

    @JsonRequest
    @ApiOperation(value = "Create task", authorizations = @Authorization(value = "master_key"))
    @Path("/create")
    public long create(@Named("project") String project, @ApiParam("name") String name, @ApiParam("script") String code, @ApiParam("parameters") Map<String, Parameter> parameters, @ApiParam("interval") Duration interval, @ApiParam(value = "image", required = false) String image)
    {
        try (Handle handle = dbi.open()) {
            GeneratedKeys<Long> longs = handle.createStatement("INSERT INTO custom_scheduled_tasks (project, name, code, schedule_interval, parameters, last_executed_at, image) VALUES (:project, :name, :code, :interval, :parameters, :updated, :image)")
                    .bind("project", project)
                    .bind("name", name)
                    .bind("image", image)
                    .bind("code", code)
                    .bind("interval", interval.getSeconds())
                    .bind("parameters", JsonHelper.encode(parameters))
                    .bind("updated", 10)
                    .executeAndReturnGeneratedKeys((index, r, ctx) -> r.getLong(1));
            return longs.first();
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
    public SuccessMessage trigger(@Named("project") String project, @ApiParam("id") int id)
    {
        LockService.Lock lock = lockService.tryLock(String.valueOf(id));
        if (lock == null) {
            return SuccessMessage.success("The task is already running");
        }

        String prefix = "scheduled-task." + id;
        JSCodeLoggerService.PersistentLogger logger = service.createLogger(project, prefix);
        CompletableFuture<Void> future;
        try {
            Map<String, Object> first;
            try (Handle handle = dbi.open()) {
                first = handle.createQuery("SELECT code, parameters FROM custom_scheduled_tasks WHERE project = :project AND id = :id")
                        .bind("project", project)
                        .bind("id", id).first();
            }

            JSConfigManager jsConfigManager = new JSConfigManager(configManager, project, prefix);

            future = run(jsCodeCompiler, executor, project,
                    first.get("code").toString(),
                    JsonHelper.read(first.get("parameters").toString(),
                            new TypeReference<Map<String, Parameter>>() {}),
                    logger,
                    jsConfigManager, eventDeserializer, eventStore, eventMappers);
        }
        catch (Throwable e) {
            lock.release();
            throw e;
        }

        future.whenComplete((events, ex) -> updateTask(project, id, lock, logger, System.currentTimeMillis(), ex));

        return SuccessMessage.success("The task is running");
    }

    private void updateTask(String project, int id, LockService.Lock lock, JSCodeCompiler.ILogger logger, long now, Throwable ex)
    {
        if (ex == null) {
            try (Handle handle = dbi.open()) {
                handle.createStatement(format("UPDATE custom_scheduled_tasks SET last_executed_at = %s WHERE project = :project AND id = :id", timestampToEpoch))
                        .bind("project", project)
                        .bind("id", id).execute();
            }
            finally {
                lock.release();
            }
        }
        else {
            lock.release();
        }

        long gapInMillis = System.currentTimeMillis() - now;
        if (ex != null) {
            logger.error(format("Failed to run the script in %d : %s", gapInMillis, ex.getMessage()));
        }
        else {
            logger.debug(format("Successfully run in %d milliseconds", gapInMillis));
        }
    }

    @JsonRequest
    @ApiOperation(value = "Update task", authorizations = @Authorization(value = "master_key"))
    @Path("/update")
    public SuccessMessage update(@Named("project") String project, @BodyParam ScheduledTask mapper)
    {
        try (Handle handle = dbi.open()) {
            int execute = handle.createStatement("UPDATE custom_scheduled_tasks SET code = :code, parameters = :parameters, schedule_interval = :interval WHERE id = :id AND project = :project")
                    .bind("project", project)
                    .bind("id", mapper.id)
                    .bind("interval", mapper.interval.getSeconds())
                    .bind("parameters", JsonHelper.encode(mapper.parameters))
                    .bind("code", mapper.script).execute();
            if (execute == 0) {
                throw new RakamException(NOT_FOUND);
            }
            return success();
        }
    }

    @JsonRequest
    @ApiOperation(value = "Test task", authorizations = @Authorization(value = "master_key"))
    @Path("/test")
    public CompletableFuture<Environment> test(@Named("project") String project, @ApiParam(value = "script") String script, @ApiParam(value = "parameters", required = false) Map<String, Parameter> parameters)
    {
        JSCodeCompiler.TestLogger logger = new JSCodeCompiler.TestLogger();
        TestingConfigManager testingConfigManager = new TestingConfigManager();
        JSCodeCompiler.IJSConfigManager ijsConfigManager = new JSConfigManager(testingConfigManager, project, null);

        InMemoryApiKeyService apiKeyService = new InMemoryApiKeyService();
        InMemoryMetastore metastore = new InMemoryMetastore(apiKeyService);
        SchemaChecker schemaChecker = new SchemaChecker(metastore, new FieldDependencyBuilder().build());
        JsonEventDeserializer testingEventDeserializer = new JsonEventDeserializer(metastore,
                apiKeyService,
                testingConfigManager,
                schemaChecker,
                fieldDependency);
        InMemoryEventStore eventStore = new InMemoryEventStore();
        metastore.createProject(project);

        CompletableFuture<Void> run = run(jsCodeCompiler, executor,
                project, script, parameters,
                logger, ijsConfigManager,
                testingEventDeserializer, eventStore, ImmutableList.of());

        scheduler.schedule(() -> {
            if (!run.isDone()) {
                run.completeExceptionally(new TimeoutException());
            }
        }, 2, MINUTES);

        CompletableFuture<Environment> resultFuture = new CompletableFuture<>();

        run.whenComplete((eventList, ex) -> {
            if (ex != null) {
                logger.info(format("Got %d events: %s: %s",
                        eventStore.getEvents().size(),
                        eventStore.getEvents().get(0).collection(),
                        eventStore.getEvents().get(0).properties()));
                logger.error("Timeouts after 120 seconds (The test execution is limited to 120 seconds)");
                return;
            }

            if (eventStore.getEvents().isEmpty()) {
                logger.info("No event is returned");
            }
            else {
                logger.info(format("Successfully got %d events: %s: %s",
                        eventStore.getEvents().size(),
                        eventStore.getEvents().get(0).collection(),
                        eventStore.getEvents().get(0).properties()));
            }

            resultFuture.complete(new Environment(logger.getEntries(), testingConfigManager.getTable().row(project)));
        });

        return resultFuture;
    }

    public static class Environment
    {
        public final List<JSCodeLoggerService.LogEntry> logs;
        public final Map<String, Object> configs;

        public Environment(List<JSCodeLoggerService.LogEntry> logs, Map<String, Object> configs)
        {
            this.logs = logs;
            this.configs = configs;
        }
    }

    static CompletableFuture<Void> run(JSCodeCompiler jsCodeCompiler, Executor executor, String project, String script, Map<String, Parameter> parameters, JSCodeCompiler.ILogger logger, JSCodeCompiler.IJSConfigManager configManager, JsonEventDeserializer deserializer, EventStore eventStore, List<EventMapper> eventMappers)
    {
        return CompletableFuture.supplyAsync(() -> {
            try {
                JSCodeCompiler.JSEventStore eventStore1 = jsCodeCompiler.getEventStore(project, deserializer, eventStore, eventMappers);
                Invocable engine = jsCodeCompiler.createEngine(script, logger, eventStore1, configManager);

                Map<String, Object> collect = Optional.ofNullable(parameters)
                        .map(v -> v.entrySet().stream()
                                .collect(Collectors.toMap(e -> e.getKey(),
                                        e -> Optional.ofNullable(e.getValue().value).orElse(""))))
                        .orElse(ImmutableMap.of());

                engine.invokeFunction("main", collect);
                return null;
            }
            catch (ScriptException e) {
                throw new RakamException("Error executing script: " + e.getMessage(), BAD_REQUEST);
            }
            catch (NoSuchMethodException e) {
                throw new RakamException("There must be a function called 'main'.", BAD_REQUEST);
            }
            catch (Throwable e) {
                throw new RakamException("Unknown error executing 'main': " + e.getMessage(), BAD_REQUEST);
            }
        }, executor);
    }

    public static class ScheduledTask
    {
        public final int id;
        public final String script;
        public final Map<String, Parameter> parameters;
        public final Instant lastUpdated;
        public final Duration interval;
        public final String name;
        public final String image;

        @JsonCreator
        public ScheduledTask(
                @ApiParam("id") int id,
                @ApiParam("name") String name,
                @ApiParam("script") String script,
                @ApiParam(value = "parameters", required = false) Map<String, Parameter> parameters,
                @ApiParam(value = "image", required = false) String image,
                @ApiParam("interval") Duration interval,
                @ApiParam(value = "last_executed_at", required = false) Instant lastUpdated)
        {
            this.id = id;
            this.name = name;
            this.script = script;
            this.parameters = parameters;
            this.image = image;
            this.interval = interval;
            this.lastUpdated = lastUpdated;
        }
    }

    private static class Task
    {
        public final String project;
        public final int id;
        public final String name;
        public final String script;
        public final Map<String, Parameter> parameters;

        private Task(String project, int id, String name, String script, Map<String, Parameter> parameters)
        {
            this.project = project;
            this.id = id;
            this.name = name;
            this.script = script;
            this.parameters = parameters;
        }
    }
}
