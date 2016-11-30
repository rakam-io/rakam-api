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

@AutoService(RakamModule.class)
public class LogModule extends RakamModule
{
    @Override
    protected void setup(Binder binder)
    {
        LogManager manager = LogManager.getLogManager();
        if (buildConfigObject(LogConfig.class).getLogActive()) {
            if (!Arrays.stream(manager.getLogger("").getHandlers())
                    .anyMatch(e -> e instanceof SentryHandler)) {
                SentryHandler sentryHandler = new SentryHandler();
                sentryHandler.setDsn("https://b507ce9416da4799a8379ddf93ec4056:10c3db26414240489f124f65b360601e@app.getsentry.com/62493");
                LogManager.getLogManager().getLogger("").addHandler(sentryHandler);
                URL gitProps = LogUtil.class.getResource("/git.properties");
                if(gitProps != null) {
                    Properties properties = new Properties();
                    try {
                        properties.load(gitProps.openStream());
                    }
                    catch (IOException e) {
                    }

                    sentryHandler.setRelease(properties.get("git.commit.id.describe").toString());
                    sentryHandler.setLevel(Level.SEVERE);
                }
                manager.getLogger("").addHandler(sentryHandler);
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

    public static class LogConfig {
        private boolean logActive = true;

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
    }
}
