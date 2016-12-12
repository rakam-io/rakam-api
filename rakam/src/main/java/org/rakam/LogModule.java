package org.rakam;

import com.getsentry.raven.jul.SentryHandler;
import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import io.airlift.configuration.Config;
import org.rakam.plugin.RakamModule;
import org.rakam.util.LogUtil;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

@AutoService(RakamModule.class)
public class LogModule
        extends RakamModule
{
    private static final String SENTRY_DSN = "https://b507ce9416da4799a8379ddf93ec4056:10c3db26414240489f124f65b360601e@app.getsentry.com/62493";

    @Override
    protected void setup(Binder binder)
    {
        LogManager manager = LogManager.getLogManager();
        LogConfig logConfig = buildConfigObject(LogConfig.class);
        if (logConfig.getLogActive()) {
            if (!Arrays.stream(manager.getLogger("").getHandlers())
                    .anyMatch(e -> e instanceof SentryHandler)) {
                Logger rootLogger = manager.getLogger("");

                SentryHandler sentryHandler = new SentryHandler();
                sentryHandler.setDsn(SENTRY_DSN);
                sentryHandler.setTags(logConfig.getTags());
                sentryHandler.setLevel(Level.SEVERE);

                URL gitProps = LogUtil.class.getResource("/git.properties");

                if (gitProps != null) {
                    Properties properties = new Properties();
                    try {
                        properties.load(gitProps.openStream());
                    }
                    catch (IOException e) {
                    }

                    sentryHandler.setRelease(properties.get("git.commit.id.describe").toString());
                }
                rootLogger.addHandler(sentryHandler);
            }
        }
    }

    @Override
    public String name()
    {
        return null;
    }

    @Override
    public String description()
    {
        return null;
    }

    public static class LogConfig
    {
        private boolean logActive = true;
        private String tags;

        @Config("log-active")
        public LogConfig setLogActive(boolean logActive)
        {
            this.logActive = logActive;
            return this;
        }

        public boolean getLogActive()
        {
            return logActive;
        }

        @Config("log-identifier")
        public LogConfig setTags(String tags)
        {
            this.tags = tags;
            return this;
        }

        public String getTags()
        {
            return tags;
        }
    }
}
