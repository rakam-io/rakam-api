package org.rakam.collection;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.ContextAttributes;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.log.Logger;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import jdk.nashorn.api.scripting.ScriptObjectMirror;
import org.rakam.analysis.ApiKeyService;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.RequestContext;
import org.rakam.plugin.EventStore;
import org.rakam.server.http.HttpRequestException;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.SwaggerJacksonAnnotationIntrospector;
import org.rakam.server.http.annotations.*;
import org.rakam.ui.WebHookUIHttpService.Parameter;
import org.rakam.util.JsonHelper;
import org.rakam.util.LogUtil;
import org.rakam.util.RakamException;
import org.rakam.util.SuccessMessage;
import org.rakam.util.javascript.JSCodeCompiler;
import org.rakam.util.javascript.JSCodeJDBCLoggerService;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;
import javax.script.Invocable;
import javax.script.ScriptException;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static org.rakam.analysis.ApiKeyService.AccessKeyType.WRITE_KEY;
import static org.rakam.server.http.HttpServer.errorMessage;
import static org.rakam.server.http.HttpServer.returnError;

@Path("/event/hook")
@Api(value = "/event/hook", nickname = "webhook", description = "Webhook for event collection", tags = "webhook")
public class WebHookHttpService
        extends HttpService {
    private final static Logger LOGGER = Logger.get(WebHookHttpService.class);

    private final DBI dbi;
    private final EventExecutorGroup executor = new DefaultEventExecutorGroup(
            Runtime.getRuntime().availableProcessors(),
            new ThreadFactoryBuilder()
                    .setNameFormat("webhook-js-executor")
                    .build());
    private final LoadingCache<WebHookIdentifier, Invocable> functions;
    private final ApiKeyService apiKeyService;
    private final EventStore eventStore;
    private final ObjectMapper jsonMapper;
    private final JSCodeCompiler jsCodeCompiler;
    private final JSCodeJDBCLoggerService loggerService;

    @Inject
    public WebHookHttpService(
            @Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource,
            JsonEventDeserializer deserializer,
            ApiKeyService apiKeyService,
            JSCodeCompiler jsCodeCompiler,
            JSCodeJDBCLoggerService loggerService,
            EventStore eventStore) {
        this.apiKeyService = apiKeyService;
        this.jsCodeCompiler = jsCodeCompiler;
        this.loggerService = loggerService;
        functions = CacheBuilder.newBuilder()
                .expireAfterWrite(5, TimeUnit.MINUTES)
                .build(new CacheLoader<WebHookIdentifier, Invocable>() {

                    @Override
                    public Invocable load(WebHookIdentifier key)
                            throws Exception {
                        WebHook webHook = get(key.project, key.identifier);
                        String prefix = "webhook." + key.project + "." + key.identifier;
                        return jsCodeCompiler.createEngine(
                                webHook.script,
                                loggerService.createLogger(key.project, prefix),
                                null,
                                jsCodeCompiler.createConfigManager(key.project, prefix), (engine, bindings) -> {
                                    Map<String, Parameter> parameters = webHook.parameters;
                                    Map<String, Object> map = new HashMap<>();
                                    parameters.forEach((k, v) -> map.put(k, v.value));

                                    bindings.put("$$params", map);
                                    try {
                                        engine.eval("var $$module = function(queryParams, body, headers) { return module(queryParams, body, $$params, headers)}");
                                    } catch (ScriptException e) {
                                        throw Throwables.propagate(e);
                                    }
                                });
                    }
                });
        this.dbi = new DBI(dataSource);
        this.eventStore = eventStore;
        jsonMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Event.class, deserializer);
        jsonMapper.registerModule(module);
        jsonMapper.registerModule(new SimpleModule("swagger", Version.unknownVersion()) {
            @Override
            public void setupModule(SetupContext context) {
                context.insertAnnotationIntrospector(new SwaggerJacksonAnnotationIntrospector());
            }
        });
    }

    @PostConstruct
    public void setup() {
        try (Handle handle = dbi.open()) {
            handle.createStatement("CREATE TABLE IF NOT EXISTS webhook (" +
                    "  project VARCHAR(255) NOT NULL," +
                    "  identifier VARCHAR(255) NOT NULL," +
                    "  code TEXT," +
                    "  active BOOLEAN NOT NULL," +
                    "  image VARCHAR(255)," +
                    "  parameters TEXT," +
                    "  PRIMARY KEY (project, identifier)" +
                    "  )")
                    .execute();
        }
    }

    @POST
    @IgnoreApi
    @ApiOperation(value = "Collect event", response = Integer.class)
    @Path("/collect/*")
    public void collectPost(RakamHttpRequest request) {
        String identifier = request.path().substring(20);
        List<String> writeKeyList = request.params().get("write_key");
        if (writeKeyList == null || writeKeyList.isEmpty()) {
            throw new RakamException("write_key query parameter is null", FORBIDDEN);
        }

        String project = apiKeyService.getProjectOfApiKey(writeKeyList.get(0), WRITE_KEY);

        request.bodyHandler(inputStream -> {
            String data;
            try {
                data = new String(ByteStreams.toByteArray(inputStream), CharsetUtil.UTF_8);
            } catch (IOException e) {
                throw new RakamException("Unable to parse data: " + e.getMessage(), INTERNAL_SERVER_ERROR);
            }

            call(request, project, identifier, request.params(), request.headers(), data);
        });
    }

    private void call(RakamHttpRequest request, String project, String identifier, Map<String, List<String>> queryParams, HttpHeaders headers, String data) {
        WebHookIdentifier key = new WebHookIdentifier(project, identifier, UUID.randomUUID().toString());
        Invocable function;
        try {
            function = functions.getUnchecked(key);
        } catch (UncheckedExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }

        Future<Object> f = executor.submit(() ->
                function.invokeFunction("$$module", queryParams, data, headers));

        f.addListener(new FutureListener<Object>() {

            @Override
            public void operationComplete(Future<Object> future)
                    throws Exception {
                if (future.await(3, TimeUnit.SECONDS)) {
                    Object body;
                    try {
                        body = future.get();
                    } catch (Throwable e) {
                        returnError(request, "Error executing callback code", INTERNAL_SERVER_ERROR);
                        LOGGER.warn(e, "Error executing webhook callback");
                        String prefix = "webhook." + key.project + "." + key.identifier;
                        String collect = headers.entries().stream()
                                .map(header -> header.getKey() + " : " + header.getValue())
                                .collect(Collectors.joining("\n"));

                        loggerService.createLogger(key.project, prefix, key.requestId)
                                .error(e.getMessage() + "\n" + request.getUri() + "\n" + collect + "Body:\n" + data + "\n--------\n");
                        return;
                    }

                    boolean saved = false;

                    if (body == null || body.equals("null")) {
                        saved = false;
                    } else {
                        if (!(body instanceof ScriptObjectMirror)) {
                            returnError(request, "The script must return an object {collection: '', properties: {}}", BAD_REQUEST);
                        }

                        ScriptObjectMirror json = (ScriptObjectMirror) ((ScriptObjectMirror) body).eval("JSON");
                        Object stringify = json.callMember("stringify", body);

                        try {
                            Event event = jsonMapper.readerFor(Event.class)
                                    .with(ContextAttributes.getEmpty()
                                            .withSharedAttribute("project", key.project))
                                    .readValue(stringify.toString());
                            if (event != null) {
                                saved = true;
                                eventStore.store(event);
                            }
                        } catch (JsonMappingException e) {
                            String message = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
                            returnError(request, "JSON couldn't parsed: " + message, BAD_REQUEST);
                            return;
                        } catch (IOException e) {
                            returnError(request, "JSON couldn't parsed: " + e.getMessage(), BAD_REQUEST);
                            return;
                        } catch (RakamException e) {
                            LogUtil.logException(request, e);
                            returnError(request, e.getMessage(), e.getStatusCode());
                            return;
                        } catch (HttpRequestException e) {
                            returnError(request, e.getMessage(), e.getStatusCode());
                            return;
                        } catch (IllegalArgumentException e) {
                            LogUtil.logException(request, e);
                            returnError(request, e.getMessage(), BAD_REQUEST);
                            return;
                        } catch (Exception e) {
                            LOGGER.error(e, "Error while collecting event");

                            returnError(request, "An error occurred", INTERNAL_SERVER_ERROR);
                            return;
                        }
                    }

                    request.response(saved ? "1" : "0").end();
                } else {
                    byte[] bytes = JsonHelper.encodeAsBytes(errorMessage("Webhook code timeouts.",
                            INTERNAL_SERVER_ERROR));

                    request.response(bytes, INTERNAL_SERVER_ERROR).end();
                }
            }
        });
    }

    @GET
    @IgnoreApi
    @ApiOperation(value = "Collect event", response = Integer.class)
    @Path("/collect/*")
    public void collectGet(RakamHttpRequest request) {
        String identifier = request.path().substring(20);
        List<String> writeKeyList = request.params().get("write_key");
        if (writeKeyList == null || writeKeyList.isEmpty()) {
            throw new RakamException("write_key query parameter is null", FORBIDDEN);
        }

        String project = apiKeyService.getProjectOfApiKey(writeKeyList.get(0), WRITE_KEY);

        call(request, project, identifier, request.params(), request.headers(), null);
    }

    @PUT
    @ApiOperation(value = "Collect event", response = Integer.class)
    @Path("/collect/*")
    @IgnoreApi
    public void collectPut(RakamHttpRequest request) {
        collectPost(request);
    }

    @ApiOperation(value = "Set hook", authorizations = @Authorization(value = "master_key"))
    @Path("/activate")
    @JsonRequest
    public SuccessMessage activate(@Named("project") RequestContext context, @BodyParam WebHook hook) {
        try (Handle handle = dbi.open()) {
            try {
                handle.createStatement("INSERT INTO webhook (project, identifier, code, active, parameters) VALUES (:project, :identifier, :code, true, :parameters)")
                        .bind("project", context.project)
                        .bind("identifier", hook.identifier)
                        .bind("code", hook.script)
                        .bind("image", hook.image)
                        .bind("parameters", JsonHelper.encode(hook.parameters))
                        .execute();
                return SuccessMessage.success();
            } catch (Exception e) {
                if (get(context, hook.identifier) != null) {
                    handle.createStatement("UPDATE webhook SET code = :code WHERE project = :project AND identifier = :identifier")
                            .bind("project", context.project)
                            .bind("identifier", hook.identifier)
                            .bind("code", hook.script)
                            .bind("image", hook.image)
                            .bind("parameters", JsonHelper.encode(hook.parameters))
                            .execute();
                    return SuccessMessage.success();
                }
                throw e;
            }
        }
    }

    @ApiOperation(value = "Delete hook", authorizations = @Authorization(value = "master_key"))
    @Path("/delete")
    @JsonRequest
    public SuccessMessage delete(@Named("project") RequestContext context, @ApiParam("identifier") String identifier) {
        try (Handle handle = dbi.open()) {
            int execute = handle.createStatement("DELETE FROM webhook WHERE project = :project AND identifier = :identifier")
                    .bind("project", context.project)
                    .bind("identifier", identifier).execute();
            if (execute == 0) {
                throw new RakamException(NOT_FOUND);
            }
            return SuccessMessage.success();
        }
    }

    @ApiOperation(value = "Get hook", authorizations = @Authorization(value = "master_key"))
    @Path("/get")
    @JsonRequest
    public WebHook get(@Named("project") RequestContext context, @ApiParam("identifier") String identifier) {
        return get(context.project, identifier);
    }

    private WebHook get(String project, String identifier) {
        try (Handle handle = dbi.open()) {
            WebHook first = handle.createQuery("SELECT code, image, active, parameters FROM webhook WHERE project = :project AND identifier = :identifier")
                    .bind("project", project)
                    .bind("identifier", identifier)
                    .map(new ResultSetMapper<WebHook>() {
                        @Override
                        public WebHook map(int index, ResultSet r, StatementContext ctx)
                                throws SQLException {
                            return new WebHook(identifier, r.getString(1),
                                    r.getString(2), r.getBoolean(3),
                                    JsonHelper.read(r.getString(4), new TypeReference<Map<String, Parameter>>() {
                                    }));
                        }
                    }).first();
            if (first == null) {
                throw new RakamException(NOT_FOUND);
            }
            return first;
        }
    }

    @ApiOperation(value = "Get hook", authorizations = @Authorization(value = "master_key"))
    @Path("/list")
    @GET
    public List<WebHook> list(@Named("project") RequestContext context) {
        try (Handle handle = dbi.open()) {
            return handle.createQuery("SELECT identifier, code, image, active, parameters FROM webhook WHERE project = :project")
                    .bind("project", context.project)
                    .map(new ResultSetMapper<WebHook>() {
                        @Override
                        public WebHook map(int index, ResultSet r, StatementContext ctx)
                                throws SQLException {
                            return new WebHook(r.getString(1), r.getString(2), r.getString(3), r.getBoolean(4), JsonHelper.read(r.getString(5), Map.class));
                        }
                    }).list();
        }
    }

    @ApiOperation(value = "Get logs", authorizations = @Authorization(value = "master_key"))
    @JsonRequest
    @Path("/get_logs")
    public List<JSCodeJDBCLoggerService.LogEntry> getLogs(@Named("project") RequestContext context, @ApiParam("identifier") String identifier, @ApiParam(value = "start", required = false) Instant start, @ApiParam(value = "end", required = false) Instant end) {
        return loggerService.getLogs(context, start, end, "webhook." + context.project + "." + identifier);
    }

    @ApiOperation(value = "Test a webhook", authorizations = @Authorization(value = "master_key"))
    @Path("/test")
    @JsonRequest
    public void test(
            RakamHttpRequest request,
            @HeaderParam(CONTENT_TYPE) String contentType,
            @Named("project") RequestContext context,
            @ApiParam("script") String script,
            @ApiParam(value = "parameters", required = false) Map<String, Object> params,
            @ApiParam(value = "body", required = false) Object body) {
        JSCodeCompiler.TestLogger testLogger = new JSCodeCompiler.TestLogger();
        JSCodeCompiler.MemoryConfigManager configManager = new JSCodeCompiler.MemoryConfigManager();
        Invocable engine;
        try {
            engine = jsCodeCompiler.createEngine(script, testLogger, null, configManager, (e, bindings) -> {
                bindings.put("$$params", params);
                try {
                    e.eval("var $$module = function(queryParams, body, headers) " +
                            "{ return module(queryParams, body, $$params, headers)}");
                } catch (ScriptException ex) {
                    throw Throwables.propagate(ex);
                }
            });
        } catch (Exception e) {
            throw new RakamException("Unable to compile Javascript code: " + e.getMessage(), INTERNAL_SERVER_ERROR);
        }

        Future<Object> f = executor.submit(() -> {
            try {
                Object scoped = engine.invokeFunction("$$module", request.params(), body, request.headers());

                if (scoped == null) {
                    request.response(script, NO_CONTENT).end();
                }
                return scoped;
            } catch (ScriptException e) {
                LOGGER.warn("Error while processing webhook script: " + e.getMessage());
                String message = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
                returnError(request, "Error while executing webhook script: " + message, BAD_REQUEST);
            } catch (Exception e) {
                LOGGER.error(e);
                returnError(request, "An error occurred.", INTERNAL_SERVER_ERROR);
            }

            return null;
        });

        f.addListener(new FutureListener<Object>() {

            @Override
            public void operationComplete(Future<Object> future)
                    throws Exception {
                if (future.await(1, TimeUnit.SECONDS)) {
                    Object body = future.getNow();
                    if (body == null) {
                        return;
                    }
                    if (!(body instanceof ScriptObjectMirror)) {
                        returnError(request, "The script must return an object or array {collection: '', properties: {}}", BAD_REQUEST);
                    }

                    ScriptObjectMirror json = (ScriptObjectMirror) ((ScriptObjectMirror) body).eval("JSON");
                    Object stringify = json.callMember("stringify", body);

                    request.response(stringify.toString()).end();
                } else {
                    byte[] bytes = JsonHelper.encodeAsBytes(errorMessage("Webhook code timeouts.",
                            INTERNAL_SERVER_ERROR));

                    request.response(bytes, INTERNAL_SERVER_ERROR).end();
                }
            }
        });
    }

    public static class WebHookIdentifier {
        public final String project;
        public final String identifier;
        public final String requestId;

        public WebHookIdentifier(String project, String identifier, String requestId) {
            this.identifier = identifier;
            this.project = project;
            this.requestId = requestId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            WebHookIdentifier that = (WebHookIdentifier) o;

            if (!project.equals(that.project)) {
                return false;
            }
            return identifier.equals(that.identifier);
        }

        @Override
        public int hashCode() {
            int result = project.hashCode();
            result = 31 * result + identifier.hashCode();
            return result;
        }
    }

    public static class WebHook {
        public final String identifier;
        public final boolean active;
        public final String script;
        public final String image;
        public final Map<String, Parameter> parameters;

        @JsonCreator
        public WebHook(
                @ApiParam("identifier") String identifier,
                @ApiParam(value = "script") String script,
                @ApiParam(value = "image", required = false) String image,
                @ApiParam(value = "active", required = false) Boolean active,
                @ApiParam(value = "parameters", required = false) Map<String, Parameter> parameters) {
            this.identifier = identifier;
            this.active = !Boolean.FALSE.equals(active);
            this.script = script;
            this.image = image;
            this.parameters = Optional.ofNullable(parameters).orElse(ImmutableMap.of());
        }
    }
}
