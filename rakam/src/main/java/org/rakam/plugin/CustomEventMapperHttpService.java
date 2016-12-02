package org.rakam.plugin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import io.airlift.log.Logger;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.DefaultCookie;
import jdk.nashorn.api.scripting.ScriptObjectMirror;
import jdk.nashorn.internal.objects.NativeDate;
import jdk.nashorn.internal.objects.NativeNumber;
import jdk.nashorn.internal.runtime.Undefined;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.Event;
import org.rakam.collection.EventCollectionHttpService;
import org.rakam.collection.EventList;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.collection.util.JSCodeCompiler;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.Api;
import org.rakam.server.http.annotations.ApiOperation;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.Authorization;
import org.rakam.server.http.annotations.BodyParam;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.ui.CustomEventMapperUIHttpService.Parameter;
import org.rakam.util.AvroUtil;
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
import static org.rakam.util.AvroUtil.generateAvroSchema;

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
    private final JSCodeCompiler jsCodeCompiler;
    private final Metastore metastore;

    @Inject
    public CustomEventMapperHttpService(
            @Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource,
            Metastore metastore,
            JSCodeCompiler jsCodeCompiler)
    {
        this.dbi = new DBI(dataSource);
        this.jsCodeCompiler = jsCodeCompiler;
        this.metastore = metastore;
        this.executor = new ThreadPoolExecutor(
                0,
                Runtime.getRuntime().availableProcessors() * 4,
                60L, SECONDS,
                new SynchronousQueue<>());

        this.scripts = CacheBuilder.newBuilder().softValues().build(new CacheLoader<String, List<JSEventMapperCompiledCode>>()
        {
            @Override
            public List<JSEventMapperCompiledCode> load(String project)
                    throws Exception
            {
                return list(project).stream().flatMap(item -> {
                    Invocable unchecked = null;
                    try {
                        unchecked = jsCodeCompiler.createEngine(project, item.code, "event-mapper." + item.id);
                    }
                    catch (Exception e) {
                        return Stream.of();
                    }

                    Map<String, Object> collect = item.parameters.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().value));
                    return Stream.of(new JSEventMapperCompiledCode(unchecked, collect));
                }).collect(Collectors.toList());
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
                    "  name VARCHAR(255) NOT NULL," +
                    "  code TEXT NOT NULL," +
                    "  image TEXT," +
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
            return handle.createQuery("SELECT id, name, code, image, parameters FROM custom_event_mappers WHERE project = :project")
                    .bind("project", project).map((index, r, ctx) -> {
                        return new JSEventMapperCode(r.getInt(1), r.getString(2), r.getString(3), r.getString(4), JsonHelper.read(r.getString(5), new TypeReference<Map<String, Parameter>>() {}));
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
    public SuccessMessage create(@Named("project") String project, @ApiParam("name") String name, @ApiParam("script") String script, @ApiParam(value = "image", required = false) String image, @ApiParam("parameters") Map<String, Parameter> parameters)
    {
        try (Handle handle = dbi.open()) {
            handle.createStatement("INSERT INTO custom_event_mappers (project, name, code, parameters, image) VALUES (:project, :name, :code, :parameters, :image)")
                    .bind("project", project)
                    .bind("code", script)
                    .bind("name", name)
                    .bind("image", image)
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
                Invocable engine = jsCodeCompiler.createEngine(project, script, null);

                DefaultHttpHeaders responseHeaders = new DefaultHttpHeaders();

                Map<String, Object> read = JsonHelper.read(requestBody, Map.class);
                Object mapper = engine.invokeFunction("mapper",
                        new TestEventsProxy(read, project),
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
                    public void set(String attr, Object value)
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
                            public void set(String attr, Object value)
                            {
                                try {
                                    f.properties().put(attr, value);
                                }
                                catch (AvroRuntimeException e) {
                                    // field not exists, create it
                                    NewField attrValue = getValue(value);

                                    if (attr.startsWith("_")) {
                                        throw new IllegalStateException("field name cannot start with _.");
                                    }

                                    List<SchemaField> fields = metastore.getOrCreateCollectionFieldList(project(), collection(),
                                            ImmutableSet.of(new SchemaField(attr, attrValue.fieldType)));
                                    GenericData.Record record = new GenericData.Record(AvroUtil.convertAvroSchema(fields));

                                    for (Schema.Field field : f.properties().getSchema().getFields()) {
                                        record.put(field.name(), f.getAttribute(field.name()));
                                    }

                                    record.put(attr, attrValue.value);
                                    f.properties(record, fields);
                                }
                            }

                            private NewField getValue(Object value)
                            {
                                if (value instanceof Undefined) {
                                    return null;
                                }
                                if (value instanceof String) {
                                    return new NewField(value, FieldType.STRING);
                                }
                                if (value instanceof Double || value instanceof Integer) {
                                    return new NewField(value, FieldType.DOUBLE);
                                }
                                if (value instanceof NativeDate) {
                                    return new NewField(NativeDate.getTime(value), FieldType.TIMESTAMP);
                                }
                                if (value instanceof NativeNumber) {
                                    return new NewField(((NativeNumber) value).doubleValue(), FieldType.DOUBLE);
                                }
                                if (value instanceof Boolean) {
                                    return new NewField(value, FieldType.BOOLEAN);
                                }
                                if (value instanceof ScriptObjectMirror) {
                                    ScriptObjectMirror mirror = (ScriptObjectMirror) value;
                                    if (mirror.isEmpty()) {
                                        return null;
                                    }
                                    if (mirror.isArray()) {
                                        Iterator<Object> iterator = mirror.values().iterator();
                                        NewField next = getValue(iterator.next());
                                        FieldType fieldType = next.fieldType;
                                        GenericArray<Object> objects = new GenericData.Array(mirror.size(),
                                                generateAvroSchema(fieldType.convertToArrayType()));
                                        objects.add(next.value);
                                        while (iterator.hasNext()) {
                                            next = getValue(iterator.next());
                                            if (next.fieldType != fieldType) {
                                                throw new IllegalStateException("Array values must have the same type.");
                                            }
                                            objects.add(next.value);
                                        }
                                    }
                                    else {
                                        HashMap<Object, Object> map = new HashMap<>(mirror.size());
                                        FieldType type = null;

                                        for (Map.Entry<String, Object> entry : mirror.entrySet()) {
                                            NewField inlineValue = getValue(entry.getValue());
                                            if (type != null && inlineValue.fieldType != type) {
                                                throw new IllegalStateException("Object values must have the same type.");
                                            }
                                            type = inlineValue.fieldType;
                                            map.put(entry.getKey(), inlineValue.value);
                                        }
                                        return new NewField(map, type.convertToMapValueType());
                                    }
                                }
                                throw new IllegalStateException();
                            }
                        };
                    }
                });
            }
        };
        return mapInternal(events.project, eventsProxy, requestParams, sourceAddress, responseHeaders);
    }

    private static class NewField
    {
        public final Object value;
        public final FieldType fieldType;

        private NewField(Object value, FieldType fieldType)
        {
            this.value = value;
            this.fieldType = fieldType;
        }
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

        void set(String attr, Object value);

        default void setOnce(String attr, Object value)
        {
            Object o = get(attr);
            if (o == null) {
                set(attr, value);
            }
        }
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
        public final String image;
        public final Map<String, Parameter> parameters;
        public final String name;

        @JsonCreator
        public JSEventMapperCode(
                @ApiParam("id") int id,
                @ApiParam("name") String name,
                @ApiParam("code") String code,
                @ApiParam("image") String image,
                @ApiParam("parameters") Map<String, Parameter> parameters)
        {
            this.id = id;
            this.name = name;
            this.code = code;
            this.image = image;
            this.parameters = parameters;
        }
    }

    public static class JSEventMapperCompiledCode
    {
        public final Invocable code;
        public final Map<String, Object> parameters;

        public JSEventMapperCompiledCode(Invocable code, Map<String, Object> parameters)
        {
            this.code = code;
            this.parameters = parameters;
        }
    }

    private static class TestEventsProxy
            implements EventsProxy
    {
        private final Map<String, Object> data;
        private final String project;

        public TestEventsProxy(Map<String, Object> data, String project)
        {
            this.data = data;
            this.project = project;
        }

        @Override
        public Event.EventContext api()
        {
            return JsonHelper.convert(data.get("api"),
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
            Map<String, Object> properties = JsonHelper.convert(data.get("properties"), Map.class);
            String collection = data.get("collection").toString();
            EventProxy value = new SingleEventProxy(collection, properties);
            return Iterators.singletonIterator(value);
        }

        private static class SingleEventProxy
                implements EventProxy
        {
            private final String collection;
            private final Map<String, Object> properties;

            public SingleEventProxy(String collection, Map<String, Object> properties)
            {
                this.collection = collection;
                this.properties = properties;
            }

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
            public void set(String attr, Object value)
            {
                properties.put(attr, value);
            }
        }
    }
}
