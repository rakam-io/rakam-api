package org.rakam.collection.util;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import jdk.nashorn.api.scripting.ClassFilter;
import jdk.nashorn.api.scripting.NashornScriptEngineFactory;
import org.rakam.analysis.ConfigManager;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.plugin.CustomEventMapperHttpService;
import org.rakam.plugin.RAsyncHttpClient;
import org.rakam.util.CryptUtil;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;
import javax.script.Bindings;
import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class JSCodeCompiler
{
    private final static Logger LOGGER = Logger.get(JSCodeCompiler.class);
    private final @Named("rakam-client") RAsyncHttpClient httpClient;
    private final ConfigManager configManager;
    private final DBI dbi;
    private final String[] args = {"-strict", "--no-syntax-extensions"};

    @Inject
    public JSCodeCompiler(
            ConfigManager configManager,
            @Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource,
            @Named("rakam-client") RAsyncHttpClient httpClient)
    {
        this.configManager = configManager;
        this.httpClient = httpClient;
        this.dbi = new DBI(dataSource);
    }

    @PostConstruct
    public void setupLogger()
    {
        try (Handle handle = dbi.open()) {
            handle.createStatement("CREATE TABLE IF NOT EXISTS javascript_logs (" +
                    "  project VARCHAR(255) NOT NULL," +
                    "  type VARCHAR(15) NOT NULL," +
                    "  prefix VARCHAR(255) NOT NULL," +
                    "  error TEXT NOT NULL," +
                    "  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP" +
                    "  )")
                    .execute();
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
        return createEngine(code,
                new PersistentLogger(project, prefix),
                prefix == null ? new MemoryConfigManager() : new JSConfigManager(configManager, project, prefix));
    }

    public PersistentLogger createLogger(String project, String prefix) {
        return new PersistentLogger(project, prefix);
    }

    public Invocable createEngine(String code, ILogger logger, IJSConfigManager configManager)
            throws ScriptException
    {
        NashornScriptEngineFactory factory = new NashornScriptEngineFactory();

        ScriptEngine engine = factory
                .getScriptEngine(args,
                        CustomEventMapperHttpService.class.getClassLoader(), new NashornEngineFilter());

        final Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
        bindings.remove("print");
        bindings.remove("load");
        bindings.remove("loadWithNewGlobal");
        bindings.remove("exit");
//        bindings.remove("Java");
        bindings.remove("quit");
        bindings.put("logger", logger);
        bindings.put("config", configManager);
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
            entries.add(new LogEntry(Level.DEBUG, value));
        }

        @Override
        public void warn(String value)
        {
            entries.add(new LogEntry(Level.WARN, value));
        }

        @Override
        public void info(String value)
        {
            entries.add(new LogEntry(Level.INFO, value));
        }

        @Override
        public void error(String value)
        {
            entries.add(new LogEntry(Level.ERROR, value));
        }

        public static class LogEntry
        {
            public final Level level;
            public final String message;

            public LogEntry(Level level, String message)
            {
                this.level = level;
                this.message = message;
            }
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

    public class PersistentLogger
            implements ILogger
    {
        private final String prefix;
        private final String project;

        public PersistentLogger(String project, String prefix)
        {
            this.project = project;
            this.prefix = prefix;
        }

        @Override
        public void debug(String value)
        {
            log("DEBUG", value);
        }

        private void log(String type, String value)
        {
            try (Handle handle = dbi.open()) {
                handle.createStatement("INSERT INTO javascript_logs (project, type, prefix, message) " +
                        "VALUES (:project, :type, :prefix, :message)")
                        .bind("project", project)
                        .bind("type", type)
                        .bind("prefix", prefix)
                        .bind("message", value)
                        .execute();
            }
        }

        @Override
        public void warn(String value)
        {
            log("WARN", value);
        }

        @Override
        public void info(String value)
        {
            log("INFO", value);
        }

        @Override
        public void error(String value)
        {
            log("ERROR", value);
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
