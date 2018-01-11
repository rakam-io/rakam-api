package org.rakam.plugin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
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
import org.rakam.analysis.RequestContext;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.*;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutorService;
import org.rakam.report.QueryResult;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.*;
import org.rakam.ui.CustomEventMapperUIHttpService.Parameter;
import org.rakam.util.AvroUtil;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;
import org.rakam.util.SuccessMessage;
import org.rakam.util.javascript.JSCodeCompiler;
import org.rakam.util.javascript.JSCodeJDBCLoggerService;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.GeneratedKeys;
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
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static java.lang.String.format;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.TimeUnit.*;
import static org.rakam.report.QueryExecutorService.DEFAULT_QUERY_RESULT_COUNT;
import static org.rakam.util.AvroUtil.generateAvroSchema;

@Path("/custom-event-mapper")
@Api(value = "/custom-event-mapper", nickname = "collection", description = "Custom event mapper", tags = "event-mapper")
public class CustomEventMapperHttpService
        extends HttpService
        implements EventMapper {
    private final DBI dbi;
    private final Logger logger = Logger.get(CustomEventMapperHttpService.class);
    private final LoadingCache<String, List<JSEventMapperCompiledCode>> scripts;
    private final ThreadPoolExecutor executor;
    private final JSCodeCompiler jsCodeCompiler;
    private final Metastore metastore;
    private final JSCodeJDBCLoggerService loggerService;
    private final QueryExecutorService queryExecutorService;

    @Inject
    public CustomEventMapperHttpService(
            @Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource,
            Metastore metastore,
            JSCodeCompiler jsCodeCompiler,
            JSCodeJDBCLoggerService loggerService,
            QueryExecutorService queryExecutorService) {
        this.dbi = new DBI(dataSource);
        this.jsCodeCompiler = jsCodeCompiler;
        this.loggerService = loggerService;
        this.metastore = metastore;
        this.executor = new ThreadPoolExecutor(
                0,
                Runtime.getRuntime().availableProcessors() * 100,
                60L, SECONDS,
                new SynchronousQueue<>());

        this.scripts = CacheBuilder.newBuilder()
                .expireAfterWrite(2, MINUTES)
                .expireAfterAccess(1, HOURS)
                .build(new MapperCodeCacheLoader());
        this.queryExecutorService = queryExecutorService;
    }

    @PostConstruct
    public void setup() {
        try (Handle handle = dbi.open()) {
            handle.createStatement("CREATE TABLE IF NOT EXISTS custom_event_mappers (" +
                    "  id SERIAL PRIMARY KEY," +
                    "  project VARCHAR(255) NOT NULL," +
                    "  name VARCHAR(255) NOT NULL," +
                    "  script TEXT NOT NULL," +
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
    public List<JSEventMapperCode> list(@Named("project") RequestContext context) {
        return list(context.project);
    }

    private List<JSEventMapperCode> list(String project) {
        try (Handle handle = dbi.open()) {
            return handle.createQuery("SELECT id, name, script, image, parameters " +
                    "FROM custom_event_mappers WHERE project = :project")
                    .bind("project", project).map((index, r, ctx) ->
                            new JSEventMapperCode(r.getInt(1), r.getString(2), r.getString(3), r.getString(4), JsonHelper.read(r.getString(5), new TypeReference<Map<String, Parameter>>() {
                            }))).list();
        }
    }

    @ApiOperation(value = "Create custom event mapper",
            authorizations = @Authorization(value = "master_key")
    )
    @Path("/update")
    @JsonRequest
    public SuccessMessage update(@Named("project") RequestContext context, @BodyParam JSEventMapperCode mapper) {
        try (Handle handle = dbi.open()) {
            int execute = handle.createStatement("UPDATE custom_event_mappers SET script = :script, parameters = :parameters, image = :image " +
                    "WHERE id = :id AND project = :project")
                    .bind("project", context.project)
                    .bind("id", mapper.id)
                    .bind("image", mapper.image)
                    .bind("parameters", JsonHelper.encode(mapper.parameters))
                    .bind("script", mapper.script).execute();
            if (execute == 0) {
                throw new RakamException(NOT_FOUND);
            }
            return SuccessMessage.success();
        }
    }

    @ApiOperation(value = "Create custom event mapper",
            authorizations = @Authorization(value = "master_key")
    )
    @Path("/create")
    @JsonRequest
    public long create(@Named("project") RequestContext context, @ApiParam("name") String name, @ApiParam("script") String script, @ApiParam(value = "image", required = false) String image, @ApiParam(value = "parameters", required = false) Map<String, Parameter> parameters) {
        try (Handle handle = dbi.open()) {
            GeneratedKeys<Long> longs = handle.createStatement("INSERT INTO custom_event_mappers (project, name, script, parameters, image) " +
                    "VALUES (:project, :name, :script, :parameters, :image)")
                    .bind("project", context.project)
                    .bind("script", script)
                    .bind("name", name)
                    .bind("image", image)
                    .bind("parameters", JsonHelper.encode(ofNullable(parameters).orElse(ImmutableMap.of())))
                    .executeAndReturnGeneratedKeys((index, r, ctx) -> r.getLong(1));
            return longs.first();
        }
    }

    @ApiOperation(value = "Delete custom event mapper",
            authorizations = @Authorization(value = "master_key")
    )
    @Path("/delete")
    @JsonRequest
    public SuccessMessage delete(@Named("project") RequestContext context, @ApiParam("id") int id) {
        try (Handle handle = dbi.open()) {
            handle.createStatement("DELETE FROM custom_event_mappers WHERE project = :project AND id = :id")
                    .bind("project", context.project)
                    .bind("id", id)
                    .execute();
            return SuccessMessage.success();
        }
    }

    @ApiOperation(value = "Get logs", authorizations = @Authorization(value = "master_key"))
    @JsonRequest
    @Path("/get_logs")
    public List<JSCodeJDBCLoggerService.LogEntry> getLogs(@Named("project") RequestContext context, @ApiParam("id") int id, @ApiParam(value = "start", required = false) Instant start, @ApiParam(value = "end", required = false) Instant end) {
        return loggerService.getLogs(context, start, end, "custom-event-mapper." + id);
    }

    @ApiOperation(value = "Test custom event mapper",
            authorizations = @Authorization(value = "master_key")
    )
    @Path("/test")
    @JsonRequest
    public CompletableFuture<TestEventMapperResult> test(
            RakamHttpRequest request,
            @Named("project") RequestContext context,
            @ApiParam("script") String script,
            @ApiParam("body") String requestBody,
            @ApiParam(value = "parameters", required = false) Map<String, Object> parameters) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                Invocable engine = jsCodeCompiler.createEngine(context.project, script, null);

                DefaultHttpHeaders responseHeaders = new DefaultHttpHeaders();

                Map<String, Object> read = JsonHelper.read(requestBody, Map.class);
                TestEventsProxy testEventsProxy = new TestEventsProxy(read, context.project);
                Object mapper = engine.invokeFunction("mapper",
                        testEventsProxy,
                        new EventCollectionHttpService.HttpRequestParams(request),
                        EventCollectionHttpService.getRemoteAddress(request.getRemoteAddress()),
                        responseHeaders,
                        new JSSQLExecutor(context.project),
                        parameters);

                if (mapper == null) {
                    return new TestEventMapperResult(testEventsProxy, null);
                }

                mapper = getValue(mapper);

                if (mapper instanceof Map) {
                    return new TestEventMapperResult(testEventsProxy, (Map) mapper);
                }

                throw new RakamException("The function didn't return a list that contains the cookie values: "
                        + JsonHelper.encode(mapper), BAD_REQUEST);
            } catch (ScriptException e) {
                throw new RakamException("Error executing script: " + e.getMessage(), BAD_REQUEST);
            } catch (NoSuchMethodException e) {
                throw new RakamException("There must be a function called 'mapper'.", BAD_REQUEST);
            } catch (Exception e) {
                throw new RakamException("Error executing JavaScript: " + e.getMessage(), BAD_REQUEST);
            }
        }, executor);
    }

    private Object getValue(Object o) {
        if (o instanceof ScriptObjectMirror) {
            ScriptObjectMirror mirror = (ScriptObjectMirror) o;
            if (mirror.isFunction()) {
                return o.toString();
            } else if (mirror.isArray()) {
                return mirror.values().stream()
                        .map(e -> getValue(e))
                        .collect(Collectors.toList());
            } else {
                return mirror.entrySet().stream()
                        .collect(Collectors.toMap(e -> e.getKey().toString(), e -> getValue(e.getValue())));
            }
        }

        return o;
    }

    @Override
    public CompletableFuture<List<Cookie>> mapAsync(Event event, RequestParams requestParams, InetAddress sourceAddress, HttpHeaders responseHeaders) {
        return mapInternal(event.project(), new EventsProxy() {
            @Override
            public Event.EventContext api() {
                return event.api();
            }

            @Override
            public String project() {
                return event.project();
            }

            @Override
            public Iterator<EventProxy> events() {
                return Iterators.singletonIterator(new ListEventProxy(event));
            }
        }, requestParams, sourceAddress, responseHeaders);
    }

    @Override
    public CompletableFuture<List<Cookie>> mapAsync(EventList events, RequestParams requestParams, InetAddress sourceAddress, HttpHeaders responseHeaders) {
        EventsProxy eventsProxy = new EventsProxy() {
            @Override
            public Event.EventContext api() {
                return events.api;
            }

            @Override
            public String project() {
                return events.project;
            }

            @Override
            public Iterator<EventProxy> events() {
                return Iterators.transform(events.events.iterator(), new Function<Event, EventProxy>() {
                    @Nullable
                    @Override
                    public EventProxy apply(@Nullable Event f) {
                        return new ListEventProxy(f);
                    }
                });
            }
        };
        return mapInternal(events.project, eventsProxy, requestParams, sourceAddress, responseHeaders);
    }

    public CompletableFuture<List<Cookie>> mapInternal(String project, EventsProxy events, RequestParams requestParams, InetAddress sourceAddress, HttpHeaders responseHeaders) {
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
                            new JSSQLExecutor(project),
                            compiledCode.parameters);
                } catch (ScriptException e) {
                    logger.warn(e, "Error executing event mapper function.");
                } catch (NoSuchMethodException e) {
                    logger.warn(e, "'mapper' function does not exist in event mapper function.");
                } catch (Throwable e) {
                    logger.warn(e, "Unknown error executing the js mapper.");
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
                            } else {
                                logger.warn(format("Event mapper didn't return a map, it returned %s", join.getClass().getName()));
                            }
                        }
                    }

                    return list;
                });
    }

    public interface EventsProxy {
        Event.EventContext api();

        String project();

        Iterator<EventProxy> events();
    }

    public interface EventProxy {
        String collection();

        Object get(String attr);

        void set(String attr, Object value);

        default void setOnce(String attr, Object value) {
            Object o = get(attr);
            if (o == null) {
                set(attr, value);
            }
        }
    }

    public static class TestEventMapperResult {
        public final TestEventsProxy event;
        public final Map<String, Object> cookies;

        public TestEventMapperResult(TestEventsProxy event, Map<String, Object> cookies) {
            this.event = event;
            this.cookies = cookies;
        }
    }

    private static class NewField {
        public final Object value;
        public final FieldType fieldType;

        private NewField(Object value, FieldType fieldType) {
            this.value = value;
            this.fieldType = fieldType;
        }
    }

    public static class JSEventMapperCode {
        public final int id;
        public final String script;
        public final String image;
        public final Map<String, Parameter> parameters;
        public final String name;

        @JsonCreator
        public JSEventMapperCode(
                @ApiParam("id") int id,
                @ApiParam("name") String name,
                @ApiParam("script") String script,
                @ApiParam("image") String image,
                @ApiParam("parameters") Map<String, Parameter> parameters) {
            this.id = id;
            this.name = name;
            this.script = script;
            this.image = image;
            this.parameters = parameters;
        }
    }

    public static class JSEventMapperCompiledCode {
        public final int id;
        public final Invocable code;
        public final Map<String, Object> parameters;
        public int codeHashCode;

        public JSEventMapperCompiledCode(int id, Invocable code, Map<String, Object> parameters, int codeHashCode) {
            this.id = id;
            this.code = code;
            this.parameters = parameters;
            this.codeHashCode = codeHashCode;
        }
    }

    private static class TestEventsProxy
            implements EventsProxy {
        private final Map<String, Object> data;
        private final String project;

        public TestEventsProxy(Map<String, Object> data, String project) {
            this.data = data;
            this.project = project;
        }

        @Override
        @JsonProperty
        public Event.EventContext api() {
            return JsonHelper.convert(data.get("api"),
                    Event.EventContext.class);
        }

        @Override
        @JsonProperty
        public String project() {
            return project;
        }

        @Override
        @JsonProperty
        public Iterator<EventProxy> events() {
            Map<String, Object> properties = JsonHelper.convert(data.get("properties"), Map.class);
            String collection = data.get("collection").toString();
            EventProxy value = new SingleEventProxy(collection, properties);
            return Iterators.singletonIterator(value);
        }

        private static class SingleEventProxy
                implements EventProxy {
            private final String collection;
            private final Map<String, Object> properties;

            public SingleEventProxy(String collection, Map<String, Object> properties) {
                this.collection = collection;
                this.properties = properties;
            }

            @Override
            @JsonProperty
            public String collection() {
                return collection;
            }

            @JsonProperty
            public Map<String, Object> properties() {
                return properties;
            }

            @Override
            public Object get(String attr) {
                return properties.get(attr);
            }

            @Override
            public void set(String attr, Object value) {
                properties.put(attr, value);
            }
        }
    }

    public class JSSQLExecutor {

        private final String project;

        public JSSQLExecutor(String project) {
            this.project = project;
        }

        public Object getOne(String queryString) throws SQLException {
            List<List<Object>> result = execute(queryString);
            return null == result ? null :
                    result.stream().findFirst().orElse(Collections.EMPTY_LIST)
                            .stream().findFirst().orElse(null);
        }

        public List<List<Object>> execute(String queryString) throws SQLException {
            QueryExecution queryExecution =
                    queryExecutorService.executeQuery(
                            project, queryString,
                            null, null, ZoneOffset.UTC, DEFAULT_QUERY_RESULT_COUNT);
            try {
                QueryResult queryResult = queryExecution.getResult().get();
                if (queryResult.isFailed()) {
                    throw new SQLException(queryResult.getError().message);
                }
                return queryResult.getResult();
            } catch (InterruptedException e) {
                throw new SQLException(e.getCause());
            } catch (ExecutionException e) {
                throw new SQLException(e.getCause());
            }
        }
    }

    private class ListEventProxy
            implements EventProxy {
        private final Event event;

        public ListEventProxy(Event event) {
            this.event = event;
        }

        @Override
        public String collection() {
            return event.collection();
        }

        @Override
        public Object get(String attr) {
            return event.getAttribute(attr);
        }

        @Override
        public void set(String attr, Object value) {
            try {
                event.properties().put(attr, value);
            } catch (AvroRuntimeException e) {
                // field not exists, create it
                NewField attrValue = getValue(value);

                if (attrValue == null) {
                    return;
                }

                List<SchemaField> fields = metastore.getOrCreateCollectionFields(event.project(), event.collection(),
                        ImmutableSet.of(new SchemaField(attr, attrValue.fieldType)));

                List<Schema.Field> oldFields = event.properties().getSchema().getFields();

                ImmutableList.Builder<Schema.Field> objectBuilder = ImmutableList.builder();

                for (Schema.Field oldField : oldFields) {
                    objectBuilder.add(new Schema.Field(oldField.name(),
                            oldField.schema(),
                            oldField.doc(),
                            oldField.defaultValue(),
                            oldField.order()));
                }

                outer:
                for (SchemaField field : fields) {
                    for (Schema.Field oldField : oldFields) {
                        if (oldField.name().equals(field.getName())) {
                            continue outer;
                        }
                    }

                    objectBuilder.add(AvroUtil.generateAvroField(field));
                }

                GenericData.Record record = new GenericData.Record(Schema.createRecord(objectBuilder.build()));

                for (Schema.Field field : event.properties().getSchema().getFields()) {
                    record.put(field.name(), event.getAttribute(field.name()));
                }

                AvroUtil.put(event.properties(), attr, attrValue.value);
                event.properties(record, fields);
            }
        }

        private NewField getValue(Object value) {
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

                    while (next == null && iterator.hasNext()) {
                        next = getValue(iterator.next());
                    }

                    if (next == null) {
                        return null;
                    }

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
                } else {
                    HashMap<Object, Object> map = new HashMap<>(mirror.size());
                    FieldType type = null;

                    for (Map.Entry<String, Object> entry : mirror.entrySet()) {
                        NewField inlineValue = getValue(entry.getValue());

                        if (inlineValue == null) {
                            continue;
                        }

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
    }

    private class MapperCodeCacheLoader
            extends CacheLoader<String, List<JSEventMapperCompiledCode>> {
        @Override
        public List<JSEventMapperCompiledCode> load(String project)
                throws Exception {
            return list(project).stream()
                    .flatMap(item -> get(project, item))
                    .collect(Collectors.toList());
        }

        private Stream<JSEventMapperCompiledCode> get(String project, JSEventMapperCode item) {
            Invocable unchecked;
            try {
                unchecked = jsCodeCompiler.createEngine(project,
                        item.script, "event-mapper." + item.id);
            } catch (Exception e) {
                return Stream.of();
            }

            return Stream.of(new JSEventMapperCompiledCode(item.id, unchecked,
                    item.parameters.entrySet()
                            .stream()
                            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().value)),
                    item.script.hashCode()));
        }

        @Override
        public ListenableFuture<List<JSEventMapperCompiledCode>> reload(String key, List<JSEventMapperCompiledCode> oldValue)
                throws Exception {
            if (oldValue == null) {
                return Futures.immediateFuture(load(key));
            } else {
                return Futures.immediateFuture(list(key).stream().flatMap(item -> {
                    for (JSEventMapperCompiledCode oldItem : oldValue) {
                        // TODO :what if the new code has same hashcode?
                        if (item.id == oldItem.id && item.script.hashCode() == oldItem.codeHashCode) {
                            return Stream.of(oldItem);
                        }
                    }
                    return get(key, item);
                }).collect(Collectors.toList()));
            }
        }
    }
}
