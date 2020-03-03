package org.rakam;

import com.google.auto.service.AutoService;
import com.google.common.base.Splitter;
import com.google.inject.Binder;
import io.airlift.configuration.Config;
import io.sentry.Sentry;
import io.sentry.SentryClient;
import io.sentry.SentryClientFactory;
import io.sentry.event.User;
import io.sentry.jul.SentryHandler;
import org.rakam.config.ProjectConfig;
import org.rakam.plugin.RakamModule;
import org.rakam.util.RakamClient;
import org.slf4j.MDC;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

@AutoService(RakamModule.class)
public class LogModule
        extends RakamModule {
    private static String HOST_NAME;

    static {
        InetAddress ip;
        try {
            ip = InetAddress.getLocalHost();
            HOST_NAME = ip.getHostName();
        } catch (UnknownHostException e) {
           //
        }
    }

    private static final String SENTRY_DSN = String.format("https://76daa36329be422ab9b592ab7239c2aa@sentry.io/1290994?release=%s&servername=%s&stacktrace.app.packages=org.rakam",
            RakamClient.RELEASE, HOST_NAME);

    @Override
    protected void setup(Binder binder) {
        LogConfig logConfig = buildConfigObject(LogConfig.class);
        ProjectConfig projectConfig = buildConfigObject(ProjectConfig.class);
        if (logConfig.getLogActive()) {
            Sentry.init(SENTRY_DSN+"&environment="+projectConfig.getCompanyName());
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
