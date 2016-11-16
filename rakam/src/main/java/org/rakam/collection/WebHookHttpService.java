package org.rakam.collection;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.ContextAttributes;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.log.Logger;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import jdk.nashorn.api.scripting.ClassFilter;
import jdk.nashorn.api.scripting.NashornScriptEngineFactory;
import org.rakam.analysis.ApiKeyService;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.plugin.EventStore;
import org.rakam.server.http.HttpRequestException;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.SwaggerJacksonAnnotationIntrospector;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.BodyParam;
import org.rakam.server.http.annotations.HeaderParam;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.AlreadyExistsException;
import org.rakam.util.CryptUtil;
import org.rakam.util.JsonHelper;
import org.rakam.util.LogUtil;
import org.rakam.util.RakamException;
import org.rakam.util.SuccessMessage;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static javax.ws.rs.core.MediaType.APPLICATION_FORM_URLENCODED;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.rakam.analysis.ApiKeyService.AccessKeyType.WRITE_KEY;
import static org.rakam.server.http.HttpServer.errorMessage;
import static org.rakam.server.http.HttpServer.returnError;

@Path("/event/hook")
@Api(value = "/event/hook", nickname = "webhook", description = "Webhook for event collection", tags = {"collect", "webhook"})
public class WebHookHttpService
        extends HttpService
{
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

    @Inject
    public WebHookHttpService(
            @Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource,
            JsonEventDeserializer deserializer,
            ApiKeyService apiKeyService,
            EventStore eventStore)
    {
        this.apiKeyService = apiKeyService;
        functions = CacheBuilder.newBuilder().softValues().build(new CacheLoader<WebHookIdentifier, Invocable>()
        {

            @Override
            public Invocable load(WebHookIdentifier key)
                    throws Exception
            {

                WebHook webHook = get(key.project, key.identifier);

                ScriptEngine engine;
                try {
                    engine = getEngine(JsonHelper.encode(webHook.parameters));
                    engine.eval(webHook.code);
                }
                catch (ScriptException e) {
                    throw new RakamException("Unable to compile JS code", INTERNAL_SERVER_ERROR);
                }

                return (Invocable) engine;
            }
        });
        this.dbi = new DBI(dataSource);
        this.eventStore = eventStore;
        jsonMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Event.class, deserializer);
        jsonMapper.registerModule(module);
        jsonMapper.registerModule(new SimpleModule("swagger", Version.unknownVersion())
        {
            @Override
            public void setupModule(SetupContext context)
            {
                context.insertAnnotationIntrospector(new SwaggerJacksonAnnotationIntrospector());
            }
        });
    }

    static class ScriptEngineFilter
            implements ClassFilter
    {
        @Override
        public boolean exposeToScripts(String s)
        {
            if (s.equals(CryptUtil.class.getName())) {
                return true;
            }
            return false;
        }
    }

    private static ScriptEngine getEngine(String params)
            throws ScriptException
    {
        ScriptEngine engine = new NashornScriptEngineFactory()
                .getScriptEngine(new String[] {"-strict", "--no-syntax-extensions"},
                        WebHookHttpService.class.getClassLoader(), new ScriptEngineFilter());

        engine.eval("" +
                "quit = function() {};\n" +
                "exit = function() {};\n" +
                //                "print = function() {};\n" +
                //                "echo = function() {};\n" +
                //                "readFully = function() {};\n" +
                //                "readLine = function() {};\n" +
                //                "load = function() {};\n" +
                "loadWithNewGlobal = function() {};\n" +
                "var scoped = function(queryParams, body, headers) { \n" +
                "   return JSON.stringify(module(queryParams, JSON.parse(body), " + params + ", headers)); \n" +
                "};\n" +
                "");

        return engine;
    }

    @PostConstruct
    public void setup()
    {
        try (Handle handle = dbi.open()) {
            handle.createStatement("CREATE TABLE IF NOT EXISTS webhook (" +
                    "  project VARCHAR(255) NOT NULL," +
                    "  identifier VARCHAR(255) NOT NULL," +
                    "  code TEXT NOT NULL," +
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
    public void collectPost(RakamHttpRequest request)
    {
        String identifier = request.path().substring(4);
        List<String> writeKeyList = request.params().get("write_key");
        if (writeKeyList == null || writeKeyList.isEmpty()) {
            throw new RakamException("write_key query parameter is null", FORBIDDEN);
        }

        String project = apiKeyService.getProjectOfApiKey(writeKeyList.get(0), WRITE_KEY);

        request.bodyHandler(inputStream -> {
            String data;
            try {
                data = new String(ByteStreams.toByteArray(inputStream), CharsetUtil.UTF_8);
            }
            catch (IOException e) {
                throw new RakamException("Unable to parse data: " + e.getMessage(), INTERNAL_SERVER_ERROR);
            }

            call(request, project, identifier, request.params(), request.headers(), data);
        });
    }

    private void call(RakamHttpRequest request, String project, String identifier, Map<String, List<String>> queryParams, HttpHeaders headers, String data)
    {
        Invocable function = functions.getUnchecked(new WebHookIdentifier(project, identifier));
        handleFunction(
                request,
                project,
                () -> function.invokeFunction("scoped", queryParams, data, headers),
                true);
    }

    @GET
    @IgnoreApi
    @ApiOperation(value = "Collect event", response = Integer.class)
    @Path("/collect/*")
    public void collectGet(RakamHttpRequest request)
    {
        String identifier = request.path().substring(4);
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
    public void collectPut(RakamHttpRequest request)
    {
        collectPost(request);
    }

    @ApiOperation(value = "Create hook", authorizations = @Authorization(value = "master_key"))
    @Path("/create")
    @JsonRequest
    public SuccessMessage create(@Named("project") String project, @BodyParam WebHook hook)
    {
        try (Handle handle = dbi.open()) {
            try {
                handle.createStatement("INSERT INTO webhook (project, identifier, code, active, parameters) VALUES (:project, :identifier, :code, true, :parameters)")
                        .bind("project", project)
                        .bind("identifier", hook.identifier)
                        .bind("code", hook.code)
                        .bind("image", hook.image)
                        .bind("parameters", JsonHelper.encode(hook.parameters))
                        .execute();
                return SuccessMessage.success();
            }
            catch (Exception e) {
                if (get(project, hook.identifier) != null) {
                    throw new AlreadyExistsException("Webhook", BAD_REQUEST);
                }
                throw e;
            }
        }
    }

    @ApiOperation(value = "Delete hook", authorizations = @Authorization(value = "master_key"))
    @Path("/delete")
    @JsonRequest
    public void delete(@Named("project") String project, @ApiParam("identifier") String identifier)
    {

    }

    @ApiOperation(value = "Get hook", authorizations = @Authorization(value = "master_key"))
    @Path("/get")
    @JsonRequest
    public WebHook get(@Named("project") String project, @ApiParam("identifier") String identifier)
    {
        try (Handle handle = dbi.open()) {
            return handle.createQuery("SELECT code, image, active, parameters FROM webhook WHERE project = :project AND identifier = :identifier")
                    .bind("project", project)
                    .bind("identifier", identifier)
                    .map(new ResultSetMapper<WebHook>() {
                        @Override
                        public WebHook map(int index, ResultSet r, StatementContext ctx)
                                throws SQLException
                        {
                            return new WebHook(identifier, r.getString(1), r.getString(2), r.getBoolean(3), JsonHelper.read(r.getString(4), Map.class));
                        }
                    }).first();
        }
    }

    @ApiOperation(value = "Get hook", authorizations = @Authorization(value = "master_key"))
    @Path("/list")
    @GET
    public List<WebHook> list(@Named("project") String project)
    {
        try (Handle handle = dbi.open()) {
            return handle.createQuery("SELECT identifier, code, image, active, parameters FROM webhook WHERE project = :project")
                    .bind("project", project)
                    .map(new ResultSetMapper<WebHook>() {
                        @Override
                        public WebHook map(int index, ResultSet r, StatementContext ctx)
                                throws SQLException
                        {
                            return new WebHook(r.getString(1), r.getString(2), r.getString(3), r.getBoolean(4), JsonHelper.read(r.getString(5), Map.class));
                        }
                    }).list();
        }
    }

    @ApiOperation(value = "Test a webhook", authorizations = @Authorization(value = "master_key"))
    @Path("/test")
    @JsonRequest
    public void test(
            RakamHttpRequest request,
            @HeaderParam(CONTENT_TYPE) String contentType,
            @Named("project") String project,
            @ApiParam("script") String script,
            @ApiParam(value = "parameters", required = false) Map<String, Object> params,
            @ApiParam(value = "body", required = false) Object body)
    {
        ScriptEngine engine;
        try {
            engine = getEngine(JsonHelper.encode(params));
            engine.eval(script);
        }
        catch (ScriptException e) {
            throw new RakamException("Unable to compile Javascript code: "+e.getMessage(), INTERNAL_SERVER_ERROR);
        }

        Future<Object> f = executor.submit(() -> {
            try {
                Object finalBody;
                if (APPLICATION_JSON.equals(contentType)) {
                    finalBody = body;
                }
                else if (APPLICATION_FORM_URLENCODED.equals(contentType)) {
                    QueryStringDecoder decoder = new QueryStringDecoder(body.toString(), false);
                    finalBody = JsonHelper.encode(decoder.parameters());
                }
                else {
                    finalBody = body;
                }

                Object scoped = ((Invocable) engine).invokeFunction("scoped",
                        request.params(),
                        finalBody,
                        request.headers());
                if (scoped == null) {
                    request.response(script, NO_CONTENT).end();
                }
                return scoped;
            }
            catch (ScriptException e) {
                LOGGER.warn("Error while processing webhook script: " + e.getMessage());
                String message = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
                returnError(request, "Error while executing webhook script: " + message, BAD_REQUEST);
            }
            catch (Exception e) {
                LOGGER.error(e);
                returnError(request, "An error occurred.", INTERNAL_SERVER_ERROR);
            }

            return null;
        });

        f.addListener(new FutureListener<Object>()
        {

            @Override
            public void operationComplete(Future<Object> future)
                    throws Exception
            {
                if (future.await(1, TimeUnit.SECONDS)) {
                    Object body = future.getNow();
                    if (body == null) {
                        return;
                    }

                    request.response(body.toString()).end();
                }
                else {
                    byte[] bytes = JsonHelper.encodeAsBytes(errorMessage("Webhook code timeouts.",
                            INTERNAL_SERVER_ERROR));

                    request.response(bytes, INTERNAL_SERVER_ERROR).end();
                }
            }
        });
    }

    private void handleFunction(RakamHttpRequest request, String project, Callable<Object> callable, boolean store)
    {
        Future<Object> f = executor.submit(callable);

        f.addListener(new FutureListener<Object>()
        {

            @Override
            public void operationComplete(Future<Object> future)
                    throws Exception
            {
                if (future.await(1, TimeUnit.SECONDS)) {
                    Object body = future.getNow();
                    if (body == null) {
                        return;
                    }
                    try {
                        Event event = jsonMapper.reader(Event.class)
                                .with(ContextAttributes.getEmpty()
                                        .withSharedAttribute("project", project))
                                .readValue(body.toString());
                        if (store) {
                            eventStore.store(event);
                        }
                    }
                    catch (JsonMappingException e) {
                        String message = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
                        returnError(request, "JSON couldn't parsed: " + message, BAD_REQUEST);
                        return;
                    }
                    catch (IOException e) {
                        returnError(request, "JSON couldn't parsed: " + e.getMessage(), BAD_REQUEST);
                        return;
                    }
                    catch (RakamException e) {
                        LogUtil.logException(request, e);
                        returnError(request, e.getMessage(), e.getStatusCode());
                        return;
                    }
                    catch (HttpRequestException e) {
                        returnError(request, e.getMessage(), e.getStatusCode());
                        return;
                    }
                    catch (IllegalArgumentException e) {
                        LogUtil.logException(request, e);
                        returnError(request, e.getMessage(), BAD_REQUEST);
                        return;
                    }
                    catch (Exception e) {
                        LOGGER.error(e, "Error while collecting event");

                        returnError(request, "An error occurred", INTERNAL_SERVER_ERROR);
                        return;
                    }

                    request.response("1").end();
                }
                else {
                    byte[] bytes = JsonHelper.encodeAsBytes(errorMessage("Webhook code timeouts.",
                            INTERNAL_SERVER_ERROR));

                    request.response(bytes, INTERNAL_SERVER_ERROR).end();
                }
            }
        });
    }

    public static class WebHookIdentifier
    {
        public final String project;
        public final String identifier;

        @JsonCreator
        public WebHookIdentifier(
                @ApiParam("project") String project,
                @ApiParam("identifier") String identifier)
        {
            this.identifier = identifier;
            this.project = project;
        }
    }

    public static class WebHook
    {
        public final String identifier;
        public final boolean active;
        public final String code;
        public final String image;
        public final Map<String, String> parameters;

        @JsonCreator
        public WebHook(
                @ApiParam("identifier") String identifier,
                @ApiParam("code") String code,
                @ApiParam("image") String image,
                @ApiParam("active") boolean active,
                @ApiParam("parameters") Map<String, String> parameters)
        {
            this.identifier = identifier;
            this.active = active;
            this.code = code;
            this.image = image;
            this.parameters = parameters;
        }
    }
}
