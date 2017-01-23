package org.rakam.collection.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.netty.handler.codec.http.HttpHeaders;
import jdk.nashorn.api.scripting.ClassFilter;
import jdk.nashorn.api.scripting.NashornScriptEngineFactory;
import org.rakam.analysis.ConfigManager;
import org.rakam.collection.Event;
import org.rakam.collection.EventCollectionHttpService;
import org.rakam.collection.EventList;
import org.rakam.collection.JSCodeLoggerService;
import org.rakam.collection.JSCodeLoggerService.LogEntry;
import org.rakam.collection.JsonEventDeserializer;
import org.rakam.plugin.CustomEventMapperHttpService;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.RAsyncHttpClient;
import org.rakam.util.CryptUtil;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import javax.inject.Named;
import javax.script.Bindings;
import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.rakam.plugin.EventMapper.RequestParams.EMPTY_PARAMS;

public class JSCodeCompiler
{
    private final static Logger LOGGER = Logger.get(JSCodeCompiler.class);
    private final @Named("rakam-client") RAsyncHttpClient httpClient;
    private final ConfigManager configManager;
    private final boolean loadAllowed;
    private final InetAddress localhost;
    private final JSCodeLoggerService loggerService;
    private static final NashornScriptEngineFactory factory = new NashornScriptEngineFactory();
    private static final String[] args = {"-strict", "--no-syntax-extensions"};
    private static final NashornEngineFilter classFilter = new NashornEngineFilter();
    private static final ClassLoader classLoader = CustomEventMapperHttpService.class.getClassLoader();

    @Inject
    public JSCodeCompiler(
            ConfigManager configManager,
            JSCodeLoggerService loggerService,
            @Named("rakam-client") RAsyncHttpClient httpClient)
    {
        this(configManager, httpClient, loggerService, false);
    }

    public JSCodeCompiler(
            ConfigManager configManager,
            @Named("rakam-client") RAsyncHttpClient httpClient,
            JSCodeLoggerService loggerService,
            boolean loadAllowed)
    {
        this.configManager = configManager;
        this.httpClient = httpClient;
        this.loggerService = loggerService;
        this.loadAllowed = loadAllowed;
        try {
            localhost = InetAddress.getLocalHost();
        }
        catch (UnknownHostException e) {
            throw Throwables.propagate(e);
        }
    }

    public class JSEventStore
    {
        private final EventStore eventStore;
        private final String project;
        private final JsonEventDeserializer jsonEventDeserializer;
        private final List<EventMapper> eventMapperSet;

        public JSEventStore(String project, JsonEventDeserializer jsonEventDeserializer, EventStore eventStore, List<EventMapper> eventMapperSet)
        {
            this.project = project;
            this.jsonEventDeserializer = jsonEventDeserializer;
            this.eventStore = eventStore;
            this.eventMapperSet = eventMapperSet;
        }

        public void store(String jsonRaw)
                throws IOException
        {
            if (jsonEventDeserializer == null) {
                throw new RakamException("Event store is not supported.", BAD_REQUEST);
            }

            JsonParser jp = JsonHelper.getMapper().getFactory().createParser(jsonRaw);
            JsonToken t = jp.nextToken();

            if (t != JsonToken.START_ARRAY) {
                throw new RakamException("The script didn't return an array", BAD_REQUEST);
            }

            t = jp.nextToken();

            List<Event> list = new ArrayList<>();
            for (; t == START_OBJECT; t = jp.nextToken()) {
                list.add(jsonEventDeserializer.deserializeWithProject(jp, project, Event.EventContext.empty(), true));
            }

            EventCollectionHttpService.mapEvent(eventMapperSet,
                    eventMapper -> eventMapper.mapAsync(new EventList(Event.EventContext.empty(), list),
                            EMPTY_PARAMS, localhost, HttpHeaders.EMPTY_HEADERS));

            eventStore.storeBatch(list);
        }
    }

    public static class NashornEngineFilter
            implements ClassFilter
    {
        private final static String CRYPT_UTIL = CryptUtil.class.getName();

        @Override
        public boolean exposeToScripts(String s)
        {
            if (s.equals(CRYPT_UTIL)) {
                return true;
            }
            return false;
        }
    }

    public Invocable createEngine(String project, String code, String prefix)
            throws ScriptException
    {
        return createEngine(
                code,
                loggerService.createLogger(project, prefix),
                null,
                prefix == null ? new MemoryConfigManager() : new JSConfigManager(configManager, project, prefix));
    }

    public JSEventStore getEventStore(String project, JsonEventDeserializer jsonEventDeserializer, EventStore eventStore, List<EventMapper> eventMappers)
    {
        return new JSEventStore(project, jsonEventDeserializer, eventStore, eventMappers);
    }

    public Invocable createEngine(String code, ILogger logger, JSEventStore eventStore, IJSConfigManager configManager)
            throws ScriptException
    {
        ScriptEngine engine = factory.getScriptEngine(args, classLoader, classFilter);
        Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);

        bindings.remove("print");
        if (!loadAllowed) {
            bindings.remove("load");
        }
        bindings.remove("loadWithNewGlobal");
        bindings.remove("exit");
//        bindings.remove("Java");
        bindings.remove("quit");
        bindings.put("logger", logger);
        bindings.put("config", configManager);
        bindings.put("$$eventStore", eventStore);
        engine.eval("var eventStore = {store: function(call) { $$eventStore.store(JSON.stringify(call)); }}");
        bindings.put("http", httpClient);

        engine.eval(code);

        return (Invocable) engine;
    }

    public interface ILogger
    {
        void debug(String value);

        void warn(String value);

        void info(String value);

        void error(String value);
    }

    public static class TestLogger
            implements ILogger
    {
        List<LogEntry> entries = new ArrayList();

        public List<LogEntry> getEntries()
        {
            return ImmutableList.copyOf(entries);
        }

        @Override
        public void debug(String value)
        {
            entries.add(new LogEntry("", Level.DEBUG, value, Instant.now()));
        }

        @Override
        public void warn(String value)
        {
            entries.add(new LogEntry("", Level.WARN, value, Instant.now()));
        }

        @Override
        public void info(String value)
        {
            entries.add(new LogEntry("", Level.INFO, value, Instant.now()));
        }

        @Override
        public void error(String value)
        {
            entries.add(new LogEntry("", Level.ERROR, value, Instant.now()));
        }
    }

    public static class JavaLogger
            implements ILogger
    {
        private final String prefix;
        private final String project;

        public JavaLogger(String project, String prefix)
        {
            this.prefix = prefix;
            this.project = project;
        }

        @Override
        public void debug(String value)
        {
            LOGGER.debug("Script(" + project + ", " + prefix + ")" + value);
        }

        @Override
        public void warn(String value)
        {
            LOGGER.warn("Script(" + project + ", " + prefix + ")" + value);
        }

        @Override
        public void info(String value)
        {
            LOGGER.info("Script(" + project + ", " + prefix + ")" + value);
        }

        @Override
        public void error(String value)
        {
            LOGGER.error("Script(" + project + ", " + prefix + ")" + value);
        }
    }

    public interface IJSConfigManager
    {
        Object get(String configName);

        void set(String configName, Object value);

        Object setOnce(String configName, Object value);
    }

    public static class MemoryConfigManager
            implements IJSConfigManager
    {
        Map<String, Object> configs = new HashMap<>();

        @Override
        public Object get(String configName)
        {
            return configs.get(configName);
        }

        @Override
        public void set(String configName, Object value)
        {
            configs.put(configName, value);
        }

        @Override
        public Object setOnce(String configName, Object value)
        {
            return configs.computeIfAbsent(configName, (k) -> value);
        }
    }

    public static class JSConfigManager
            implements IJSConfigManager
    {
        private final ConfigManager configManager;
        private final String project;
        private final String prefix;

        public JSConfigManager(ConfigManager configManager, String project, String prefix)
        {
            this.configManager = configManager;
            this.project = project;
            this.prefix = Optional.ofNullable(prefix).map(v -> v + ".").orElse("");
        }

        @Override
        public Object get(String configName)
        {
            return configManager.getConfig(project, prefix + configName, Object.class);
        }

        @Override
        public void set(String configName, Object value)
        {
            configManager.setConfig(project, prefix + configName, value);
        }

        @Override
        public Object setOnce(String configName, Object value)
        {
            return configManager.setConfigOnce(project, prefix + configName, value);
        }
    }
}
