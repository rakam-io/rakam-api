package org.rakam.plugin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Function;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterators;
import io.airlift.log.Logger;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.DefaultCookie;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.collection.Event;
import org.rakam.collection.EventCollectionHttpService;
import org.rakam.collection.EventList;
import org.rakam.collection.util.JSCodeCompiler;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
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

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;
import javax.script.Invocable;
import javax.script.ScriptException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

@Path("/custom-event-mapper")
@Api(value = "/custom-event-mapper", nickname = "collection", description = "Custom event mapper", tags = "collection")
public class CustomEventMapperHttpService
        extends HttpService
        implements EventMapper
{
    private final DBI dbi;
    private final Logger logger = Logger.get(CustomEventMapperHttpService.class);
    private final LoadingCache<String, List<JSEventMapperCompiledCode>> scripts;
    private final ThreadPoolExecutor executor;
    private final LoadingCache<String, Invocable> codeCache;
    private final JSCodeCompiler jsCodeCompiler;

    @Inject
    public CustomEventMapperHttpService(
            @Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource,
            JSCodeCompiler jsCodeCompiler)
    {
        this.dbi = new DBI(dataSource);
        this.jsCodeCompiler = jsCodeCompiler;
        this.executor = new ThreadPoolExecutor(
                0,
                Runtime.getRuntime().availableProcessors() * 4,
                60L, SECONDS,
                new SynchronousQueue<>());

        this.scripts = CacheBuilder.newBuilder().softValues().build(new CacheLoader<String, List<JSEventMapperCompiledCode>>()
        {
            @Override
            public List<JSEventMapperCompiledCode> load(String key)
                    throws Exception
            {
                return list(key).stream().flatMap(item -> {
                    Invocable unchecked = null;
                    try {
                        unchecked = codeCache.getUnchecked(item.code);
                    }
                    catch (Exception e) {
                        return Stream.of();
                    }
                    return Stream.of(new JSEventMapperCompiledCode(unchecked, item.parameters));
                }).collect(Collectors.toList());
            }
        });

        this.codeCache = CacheBuilder.newBuilder().softValues().build(new CacheLoader<String, Invocable>()
        {
            @Override
            public Invocable load(String code)
                    throws Exception
            {
                return jsCodeCompiler.createEngine(code);
            }
        });
    }



    @PostConstruct
    public void setup()
    {
        try (Handle handle = dbi.open()) {
            handle.createStatement("CREATE TABLE IF NOT EXISTS custom_event_mappers (" +
                    "  id SERIAL PRIMARY KEY," +
                    "  project VARCHAR(255) NOT NULL," +
                    "  code TEXT NOT NULL," +
                    "  parameters TEXT" +
                    "  )")
                    .execute();
        }
    }

    @ApiOperation(value = "List custom event mappers",
            authorizations = @Authorization(value = "master_key")
    )
    @GET
    @Path("/list")
    @JsonRequest
    public List<JSEventMapperCode> list(@Named("project") String project)
    {
        try (Handle handle = dbi.open()) {
            return handle.createQuery("SELECT id, code, parameters FROM custom_event_mappers WHERE project = :project")
                    .bind("project", project).map((index, r, ctx) -> {
                        return new JSEventMapperCode(r.getInt(1), r.getString(1), JsonHelper.read(r.getString(2)));
                    }).list();
        }
    }

    @ApiOperation(value = "Create custom event mapper",
            authorizations = @Authorization(value = "master_key")
    )
    @Path("/update")
    @JsonRequest
    public SuccessMessage update(@Named("project") String project, @BodyParam JSEventMapperCode mapper)
    {
        try (Handle handle = dbi.open()) {
            handle.createStatement("UPDATE custom_event_mappers SET code = :code AND parameters = :paremeters WHERE id = :id AND project = :project")
                    .bind("project", project)
                    .bind("id", mapper.id)
                    .bind("parameters", mapper.parameters)
                    .bind("code", mapper.code);
            return SuccessMessage.success();
        }
    }

    @ApiOperation(value = "Create custom event mapper",
            authorizations = @Authorization(value = "master_key")
    )
    @Path("/create")
    @JsonRequest
    public SuccessMessage create(@Named("project") String project, @ApiParam("code") String code, @ApiParam("parameters") Map<String, Object> parameters)
    {
        try (Handle handle = dbi.open()) {
            handle.createStatement("INSERT INTO custom_event_mappers (project, code, parameters) VALUES (:project, :code, :parameters)")
                    .bind("project", project)
                    .bind("code", code)
                    .bind("parameters", JsonHelper.encode(parameters))
                    .execute();
            return SuccessMessage.success();
        }
    }

    @ApiOperation(value = "Delete custom event mapper",
            authorizations = @Authorization(value = "master_key")
    )
    @Path("/delete")
    @JsonRequest
    public SuccessMessage delete(@Named("project") String project, @ApiParam("id") int id)
    {
        try (Handle handle = dbi.open()) {
            handle.createStatement("DELETE FROM custom_event_mappers WHERE project = :project AND id = :id")
                    .bind("project", project)
                    .bind("code", id)
                    .execute();
            return SuccessMessage.success();
        }
    }

    @ApiOperation(value = "Test custom event mapper",
            authorizations = @Authorization(value = "master_key")
    )
    @Path("/test")
    @JsonRequest
    public CompletableFuture<Map<String, String>> test(RakamHttpRequest request,
            @Named("project") String project,
            @ApiParam("script") String script,
            @ApiParam("body") String requestBody,
            @ApiParam(value = "parameters", required = false) Map<String, Object> parameters)
    {
        return CompletableFuture.supplyAsync(() -> {
            try {
                Invocable engine = jsCodeCompiler.createEngine(script);

                DefaultHttpHeaders responseHeaders = new DefaultHttpHeaders();

                Map<String, Object> read = JsonHelper.read(requestBody, Map.class);
                Object mapper = engine.invokeFunction("mapper",
                        new EventsProxy()
                        {
                            @Override
                            public Event.EventContext api()
                            {
                                return JsonHelper.convert(read.get("api"),
                                        Event.EventContext.class);
                            }

                            @Override
                            public String project()
                            {
                                return project;
                            }

                            @Override
                            public Iterator<EventProxy> events()
                            {
                                Map<String, String> properties = JsonHelper.convert(read.get("properties"), Map.class);
                                String collection = read.get("collection").toString();
                                return Iterators.singletonIterator(new EventProxy()
                                {
                                    @Override
                                    public String collection()
                                    {
                                        return collection;
                                    }

                                    @Override
                                    public Object get(String attr)
                                    {
                                        return properties.get(attr);
                                    }

                                    @Override
                                    public void set(String attr, int value)
                                    {
                                        System.out.println(1);
                                    }
                                });
                            }
                        },
                        new EventCollectionHttpService.HttpRequestParams(request),
                        EventCollectionHttpService.getRemoteAddress(request.getRemoteAddress()),
                        responseHeaders,
                        parameters);

                if (mapper == null) {
                    return null;
                }

                if (mapper instanceof Map) {
                    HashMap<String, String> map = new HashMap<>();
                    ((Map) mapper).forEach((o, o2) ->
                            map.put(o.toString(), o2.toString()));
                    return map;
                }

                throw new RakamException("The function didn't return an object that represents the cookie value: "
                        + JsonHelper.encode(mapper), BAD_REQUEST);
            }
            catch (ScriptException e) {
                throw new RakamException("Error executing script: " + e.getMessage(), BAD_REQUEST);
            }
            catch (NoSuchMethodException e) {
                throw new RakamException("There must be a function called 'mapper'.", BAD_REQUEST);
            }
        }, executor);
    }

    @Override
    public CompletableFuture<List<Cookie>> mapAsync(Event event, RequestParams requestParams, InetAddress sourceAddress, HttpHeaders responseHeaders)
    {
        return mapInternal(event.project(), new EventsProxy()
        {
            @Override
            public Event.EventContext api()
            {
                return event.api();
            }

            @Override
            public String project()
            {
                return event.project();
            }

            @Override
            public Iterator<EventProxy> events()
            {
                return Iterators.singletonIterator(new EventProxy()
                {
                    @Override
                    public String collection()
                    {
                        return event.collection();
                    }

                    @Override
                    public String get(String attr)
                    {
                        return event.getAttribute(attr);
                    }

                    @Override
                    public void set(String attr, int value)
                    {
                        return;
                    }
                });
            }
        }, requestParams, sourceAddress, responseHeaders);
    }

    @Override
    public CompletableFuture<List<Cookie>> mapAsync(EventList events, RequestParams requestParams, InetAddress sourceAddress, HttpHeaders responseHeaders)
    {
        EventsProxy eventsProxy = new EventsProxy()
        {
            @Override
            public Event.EventContext api()
            {
                return events.api;
            }

            @Override
            public String project()
            {
                return events.project;
            }

            @Override
            public Iterator<EventProxy> events()
            {
                return Iterators.transform(events.events.iterator(), new Function<Event, EventProxy>()
                {
                    @Nullable
                    @Override
                    public EventProxy apply(@Nullable Event f)
                    {
                        return new EventProxy()
                        {
                            @Override
                            public String collection()
                            {
                                return f.collection();
                            }

                            @Override
                            public String get(String attr)
                            {
                                return f.getAttribute(attr);
                            }

                            @Override
                            public void set(String attr, int value)
                            {
                                return;
                            }
                        };
                    }
                });
            }
        };
        return mapInternal(events.project, eventsProxy, requestParams, sourceAddress, responseHeaders);
    }

    public interface EventsProxy
    {
        Event.EventContext api();

        String project();

        Iterator<EventProxy> events();
    }

    public interface EventProxy
    {
        String collection();

        Object get(String attr);

        void set(String attr, int value);
    }

    public CompletableFuture<List<Cookie>> mapInternal(String project, EventsProxy events, RequestParams requestParams, InetAddress sourceAddress, HttpHeaders responseHeaders)
    {
        List<JSEventMapperCompiledCode> unchecked = scripts.getUnchecked(project);
        CompletableFuture<Object>[] futures = new CompletableFuture[unchecked.size()];
        for (int i = 0; i < unchecked.size(); i++) {
            JSEventMapperCompiledCode compiledCode = unchecked.get(i);

            futures[i] = CompletableFuture.supplyAsync(() -> {
                try {
                    return compiledCode.code.invokeFunction("mapper",
                            events,
                            requestParams,
                            sourceAddress,
                            responseHeaders,
                            compiledCode.parameters);
                }
                catch (ScriptException e) {
                    logger.warn(e, "Error executing event mapper function.");
                }
                catch (NoSuchMethodException e) {
                    logger.warn(e, "'mapper' function does not exist in event mapper function.");
                }

                return null;
            }, executor);
        }

        return CompletableFuture.allOf(futures)
                .thenApply(aVoid -> {
                    List<Cookie> list = new ArrayList<>();

                    for (CompletableFuture<Object> result : futures) {
                        Object join = result.join();
                        if (join != null) {
                            if (join instanceof Map) {
                                ((Map) join).forEach((o, o2) ->
                                        list.add(new DefaultCookie(o.toString(), o2.toString())));
                            }
                            else {
                                logger.warn(format("Event mapper didn't return a map, it returned %s", join.getClass().getName()));
                            }
                        }
                    }

                    return list;
                });
    }

    public static class JSEventMapperCode
    {
        public final int id;
        public final String code;
        public final Map<String, String> parameters;

        @JsonCreator
        public JSEventMapperCode(
                @ApiParam("id") int id,
                @ApiParam("code") String code,
                @ApiParam("parameters") Map<String, String> parameters)
        {
            this.id = id;
            this.code = code;
            this.parameters = parameters;
        }
    }

    public static class JSEventMapperCompiledCode
    {
        public final Invocable code;
        public final Map<String, String> parameters;

        public JSEventMapperCompiledCode(Invocable code, Map<String, String> parameters)
        {
            this.code = code;
            this.parameters = parameters;
        }
    }
}
