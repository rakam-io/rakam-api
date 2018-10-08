package org.rakam;

import com.google.auto.service.AutoService;
import com.google.common.base.Splitter;
import com.google.inject.Binder;
import io.airlift.configuration.Config;
import io.sentry.Sentry;
import io.sentry.SentryClient;
import io.sentry.jul.SentryHandler;
import org.rakam.plugin.RakamModule;
import org.rakam.util.RakamClient;

import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

@AutoService(RakamModule.class)
public class LogModule
        extends RakamModule {
    private static final String SENTRY_DSN = "https://76daa36329be422ab9b592ab7239c2aa@sentry.io/1290994";

    @Override
    protected void setup(Binder binder) {
        LogManager manager = LogManager.getLogManager();
        LogConfig logConfig = buildConfigObject(LogConfig.class);
        if (logConfig.getLogActive()) {
            if (!Arrays.stream(manager.getLogger("").getHandlers())
                    .anyMatch(e -> e instanceof SentryHandler)) {
                Logger rootLogger = manager.getLogger("");

                SentryClient client = Sentry.init(SENTRY_DSN);
                if (logConfig.getTags() != null) {
                    for (String item : Splitter.on(',').split(logConfig.getTags())) {
                        String[] split = item.split("=", 2);
                        client.addTag(split[0], split.length > 1 ? split[1] : "true");
                    }
                }
                client.setRelease(RakamClient.RELEASE);

                SentryHandler sentryHandler = new SentryHandler();
                sentryHandler.setLevel(Level.SEVERE);
                rootLogger.addHandler(sentryHandler);
            }
        }
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public String description() {
        return null;
    }

    public static class LogConfig {
        private boolean logActive = true;
        private String tags;

        public boolean getLogActive() {
            return logActive;
        }

        @Config("log-active")
        public LogConfig setLogActive(boolean logActive) {
            this.logActive = logActive;
            return this;
        }

        public String getTags() {
            return tags;
        }

        @Config("log-identifier")
        public LogConfig setTags(String tags) {
            this.tags = tags;
            return this;
        }
    }
}
