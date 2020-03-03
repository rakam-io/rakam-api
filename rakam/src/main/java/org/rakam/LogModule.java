package org.rakam;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import io.airlift.configuration.Config;
import io.sentry.Sentry;
import io.sentry.event.User;
import org.rakam.config.ProjectConfig;
import org.rakam.plugin.RakamModule;
import org.rakam.util.RakamClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

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

    @Override
    protected void setup(Binder binder) {
        LogConfig logConfig = buildConfigObject(LogConfig.class);
        ProjectConfig projectConfig = buildConfigObject(ProjectConfig.class);
        if (logConfig.getLogActive()) {
            if(projectConfig.getCompanyName() != null) {
                Sentry.getStoredClient().setEnvironment(projectConfig.getCompanyName());
            }
            Sentry.getStoredClient().setRelease(RakamClient.RELEASE);
            Sentry.getStoredClient().setServerName(HOST_NAME);
            Sentry.init(logConfig.getSentryUri() + "?stacktrace.app.packages=org.rakam");
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
        private static final String PUBLIC_SENTRY_DSN = "https://76daa36329be422ab9b592ab7239c2aa@sentry.io/1290994";

        private boolean logActive = true;
        private String sentryUri = PUBLIC_SENTRY_DSN;

        public boolean getLogActive() {
            return logActive;
        }

        @Config("log-active")
        public LogConfig setLogActive(boolean logActive) {
            this.logActive = logActive;
            return this;
        }

        @Config("sentry-uri")
        public LogConfig setSentryUri(String sentryUri) {
            this.sentryUri = sentryUri;
            return this;
        }

        public String getSentryUri() {
            return sentryUri;
        }
    }
}
