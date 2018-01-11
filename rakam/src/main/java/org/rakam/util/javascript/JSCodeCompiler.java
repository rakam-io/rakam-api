package org.rakam.util.javascript;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.CharsetUtil;
import jdk.nashorn.api.scripting.ClassFilter;
import jdk.nashorn.api.scripting.NashornScriptEngineFactory;
import org.rakam.analysis.ConfigManager;
import org.rakam.collection.Event;
import org.rakam.collection.EventCollectionHttpService;
import org.rakam.collection.EventList;
import org.rakam.collection.JsonEventDeserializer;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.EventStore;
import org.rakam.util.CryptUtil;
import org.rakam.util.JsonHelper;
import org.rakam.util.RAsyncHttpClient;
import org.rakam.util.RakamException;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import javax.script.*;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URL;
import java.time.Instant;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;
import static org.rakam.plugin.EventMapper.RequestParams.EMPTY_PARAMS;

public class JSCodeCompiler {
    private final static Logger LOGGER = Logger.get(JSCodeCompiler.class);
    private static final NashornScriptEngineFactory factory = new NashornScriptEngineFactory();
    private static final String[] args = {"-strict", "--no-syntax-extensions"};
    private static final NashornEngineFilter classFilter = new NashornEngineFilter();
    private static final ClassLoader classLoader = new SafeClassLoader() {
    };
    private static Map<String, Object> JS_UTIL = ImmutableMap.of(
            "crypt", new JSUtil.JSCryptUtil(),
            "base64", new JSUtil.JSBase64Util(),
            "request", new JSUtil.JSRequestUtil());
    private final @Named("rakam-client")
    RAsyncHttpClient httpClient;
    private final ConfigManager configManager;
    private final boolean loadAllowed;
    private final InetAddress localhost;
    private final LoggerFactory loggerService;
    private final boolean customEnabled;

    @Inject
    public JSCodeCompiler(
            ConfigManager configManager,
            @Named("rakam-client") RAsyncHttpClient httpClient,
            JSLoggerService loggerService,
            JavascriptConfig config) {
        this(configManager, httpClient,
                (project, prefix) -> loggerService.createLogger(project, prefix),
                false, config.getCustomEnabled());
    }

    public JSCodeCompiler(
            ConfigManager configManager,
            @Named("rakam-client") RAsyncHttpClient httpClient,
            LoggerFactory loggerService,
            boolean loadAllowed,
            boolean customEnabled) {
        this.configManager = configManager;
        this.httpClient = httpClient;
        this.loggerService = loggerService;
        this.loadAllowed = loadAllowed;
        this.customEnabled = customEnabled;
        localhost = InetAddress.getLoopbackAddress();
    }

    public Invocable createEngine(String project, String code, String prefix)
            throws ScriptException {

        return createEngine(
                code,
                loggerService.createLogger(project, prefix),
                null,
                prefix == null ? new MemoryConfigManager() : createConfigManager(project, prefix));
    }

    public JSConfigManager createConfigManager(String project, String prefix) {
        return new JSConfigManager(configManager, project, prefix);
    }

    public JSEventStore getEventStore(String project, JsonEventDeserializer jsonEventDeserializer, EventStore eventStore, List<EventMapper> eventMappers, ILogger logger) {
        return new JSEventStore(project, jsonEventDeserializer, eventStore, eventMappers, logger);
    }

    public Invocable createEngine(String code, ILogger logger, JSEventStore eventStore, IJSConfigManager configManager)
            throws ScriptException {
        return createEngine(code, logger, eventStore, configManager, (scriptEngine, bindings) -> {

        });
    }

    public Invocable createEngine(String code, ILogger logger, JSEventStore eventStore, IJSConfigManager configManager, BiConsumer<ScriptEngine, Bindings> binding)
            throws ScriptException {
        if (!customEnabled) {
            int firstLineBreak = code.indexOf("\n");
            if (firstLineBreak == -1) {
                throw new RakamException("Custom javascript code is not allowed in trial mode.", BAD_REQUEST);
            }
            String substring = code.substring(0, firstLineBreak);
            if (!substring.startsWith("//@ sourceURL=rakam-ui/src/main/resources/")) {
                throw new RakamException("Custom javascript code is not allowed in trial mode.", BAD_REQUEST);
            }

            String path = substring.substring("//@ sourceURL=rakam-ui/src/main/resources/".length());
        }
        ScriptEngine engine = factory.getScriptEngine(args, classLoader, classFilter);
        Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);

        bindings.remove("print");
        if (!loadAllowed) {
            bindings.remove("load");
        }
        bindings.remove("loadWithNewGlobal");
        bindings.remove("exit");
        bindings.remove("Java");
        bindings.remove("readFully");
        bindings.remove("readLine");
        bindings.remove("print");
        bindings.remove("echo");
        bindings.remove("quit");
        bindings.put("logger", logger);
        bindings.put("util", JS_UTIL);
        bindings.put("config", configManager);
        if (eventStore != null) {
            bindings.put("$$eventStore", eventStore);
            engine.eval("var eventStore = {store: function(call) { $$eventStore.store(JSON.stringify(call)); }}");
        }
        bindings.put("http", httpClient);

        engine.eval(code);
        binding.accept(engine, bindings);

        return (Invocable) engine;
    }

    public interface LoggerFactory {
        ILogger createLogger(String project, String prefix);
    }

    public interface IJSConfigManager {
        Object get(String configName);

        void set(String configName, Object value);

        Object setOnce(String configName, Object value);
    }

    public static class NashornEngineFilter
            implements ClassFilter {
        @Override
        public boolean exposeToScripts(String s) {
            return false;
        }
    }

    public static class TestLogger
            implements ILogger {
        List<JSLoggerService.LogEntry> entries = new ArrayList();

        public List<JSLoggerService.LogEntry> getEntries() {
            return ImmutableList.copyOf(entries);
        }

        @Override
        public void debug(String value) {
            entries.add(new JSLoggerService.LogEntry("", Level.DEBUG, value, Instant.now()));
        }

        @Override
        public void warn(String value) {
            entries.add(new JSLoggerService.LogEntry("", Level.WARN, value, Instant.now()));
        }

        @Override
        public void info(String value) {
            entries.add(new JSLoggerService.LogEntry("", Level.INFO, value, Instant.now()));
        }

        @Override
        public void error(String value) {
            entries.add(new JSLoggerService.LogEntry("", Level.ERROR, value, Instant.now()));
        }
    }

    public static class JavaLogger
            implements ILogger {
        private final String prefix;
        private final String project;

        public JavaLogger(String project, String prefix) {
            this.prefix = prefix;
            this.project = project;
        }

        @Override
        public void debug(String value) {
            LOGGER.debug("Script(" + project + ", " + prefix + ")" + value);
        }

        @Override
        public void warn(String value) {
            LOGGER.warn("Script(" + project + ", " + prefix + ")" + value);
        }

        @Override
        public void info(String value) {
            LOGGER.info("Script(" + project + ", " + prefix + ")" + value);
        }

        @Override
        public void error(String value) {
            LOGGER.error("Script(" + project + ", " + prefix + ")" + value);
        }
    }

    public static class MemoryConfigManager
            implements IJSConfigManager {
        Map<String, Object> configs = new HashMap<>();

        @Override
        public Object get(String configName) {
            return configs.get(configName);
        }

        @Override
        public void set(String configName, Object value) {
            configs.put(configName, value);
        }

        @Override
        public Object setOnce(String configName, Object value) {
            return configs.computeIfAbsent(configName, (k) -> value);
        }
    }

    private static class JSUtil {
        private JSUtil() {
        }

        public static class JSRequestUtil {
            public Map<String, List<String>> parseFormData(String data) {
                return new QueryStringDecoder("?" + data).parameters();
            }
        }

        public static class JSCryptUtil {
            public String generateRandomKey(int length) {
                return CryptUtil.generateRandomKey(length);
            }

            public String sha1(String value) {
                return CryptUtil.sha1(value);
            }

            public String encryptWithHMacSHA1(String data, String secret) {
                return CryptUtil.encryptWithHMacSHA1(data, secret);
            }

            public String encryptToHex(String data, String secret, String hashType) {
                return CryptUtil.encryptToHex(data, secret, hashType);
            }

            public String encryptAES(String data, String secretKey) {
                return CryptUtil.encryptAES(data, secretKey);
            }

            public String decryptAES(String data, String secretKey) {
                return CryptUtil.decryptAES(data, secretKey);
            }
        }

        public static class JSBase64Util {
            public String encode(String data) {
                return Base64.getEncoder().encodeToString(data.getBytes(CharsetUtil.UTF_8));
            }
        }
    }

    public static class SafeClassLoader
            extends ClassLoader {
        @Override
        public Class<?> loadClass(String name)
                throws ClassNotFoundException {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve)
                throws ClassNotFoundException {
            return super.loadClass(name, resolve);
        }

        @Override
        protected Object getClassLoadingLock(String className) {
            return super.getClassLoadingLock(className);
        }

        @Override
        public void clearAssertionStatus() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Class<?> findClass(String name)
                throws ClassNotFoundException {
            throw new UnsupportedOperationException();
        }

        @Nullable
        @Override
        public URL getResource(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Enumeration<URL> getResources(String name)
                throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        protected URL findResource(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Enumeration<URL> findResources(String name)
                throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public InputStream getResourceAsStream(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Package definePackage(String name, String specTitle, String specVersion, String specVendor, String implTitle, String implVersion, String implVendor, URL sealBase)
                throws IllegalArgumentException {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Package getPackage(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Package[] getPackages() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected String findLibrary(String libname) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setDefaultAssertionStatus(boolean enabled) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setPackageAssertionStatus(String packageName, boolean enabled) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setClassAssertionStatus(String className, boolean enabled) {
            throw new UnsupportedOperationException();
        }
    }

    public class JSEventStore {
        private final EventStore eventStore;
        private final String project;
        private final JsonEventDeserializer jsonEventDeserializer;
        private final List<EventMapper> eventMapperSet;
        private final ILogger logger;

        public JSEventStore(String project, JsonEventDeserializer jsonEventDeserializer, EventStore eventStore, List<EventMapper> eventMapperSet, ILogger logger) {
            this.project = project;
            this.jsonEventDeserializer = jsonEventDeserializer;
            this.eventStore = eventStore;
            this.eventMapperSet = eventMapperSet;
            this.logger = logger;
        }

        public void store(String jsonRaw)
                throws IOException {
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

            int[] ints = eventStore.storeBatch(list);
            if (ints.length > 0) {
                logger.error(format("Failed to save events: %s", Arrays.stream(ints).boxed().map(i -> i + "").collect(Collectors.joining(", "))));
            }
        }
    }
}
