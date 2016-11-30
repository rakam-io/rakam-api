package org.rakam.util;

import com.getsentry.raven.Raven;
import com.getsentry.raven.RavenFactory;
import com.getsentry.raven.event.Event;
import com.getsentry.raven.event.EventBuilder;
import com.getsentry.raven.event.interfaces.ExceptionInterface;
import com.getsentry.raven.event.interfaces.HttpInterface;
import com.getsentry.raven.jul.SentryHandler;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.report.QueryError;
import org.rakam.report.QueryExecutor;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.RakamServletWrapper;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.stream.Collectors;

public class LogUtil
{
    private static final Raven RAVEN;
    private static final Map<String, String> TAGS;
    private static final String RELEASE;

    static {
        LogManager manager = LogManager.getLogManager();
        String canonicalName = SentryHandler.class.getCanonicalName();
        String dsnInternal = manager.getProperty(canonicalName + ".dsn");
        String tagsString = manager.getProperty(canonicalName + ".tags");

        TAGS = Optional.ofNullable(tagsString).map(str ->
                Arrays.stream(str.split(",")).map(val -> val.split(":")).collect(Collectors.toMap(a -> a[0], a -> a[1])))
                .orElse(ImmutableMap.of());

        RELEASE = manager.getProperty(canonicalName + ".release");

        RAVEN = dsnInternal != null ? RavenFactory.ravenInstance(dsnInternal) : null;
    }

    public static void logException(RakamHttpRequest request, RakamException e)
    {
        if (RAVEN == null) {
            return;
        }
        EventBuilder builder = new EventBuilder()
                .withMessage(e.getMessage())
                .withSentryInterface(new HttpInterface(new RakamServletWrapper(request)))
                .withLevel(Event.Level.WARNING)
                .withLogger(RakamException.class.getName())
                .withTag("status", e.getStatusCode().reasonPhrase());

        if (TAGS != null) {
            for (Map.Entry<String, String> entry : TAGS.entrySet()) {
                builder.withTag(entry.getKey(), entry.getValue());
            }
        }

        if (RELEASE != null) {
            builder.withRelease(RELEASE);
        }

        RAVEN.sendEvent(builder.build());
    }

    public static void logException(RakamHttpRequest request, Throwable e)
    {
        if (RAVEN == null) {
            return;
        }
        EventBuilder builder = new EventBuilder()
                .withSentryInterface(new ExceptionInterface(e))
                .withSentryInterface(new HttpInterface(new RakamServletWrapper(request)))
                .withLevel(Event.Level.WARNING)
                .withLogger(RakamException.class.getName());

        if (TAGS != null) {
            for (Map.Entry<String, String> entry : TAGS.entrySet()) {
                builder.withTag(entry.getKey(), entry.getValue());
            }
        }

        if (RELEASE != null) {
            builder.withRelease(RELEASE);
        }

        RAVEN.sendEvent(builder.build());
    }

    public static void logException(RakamHttpRequest request, IllegalArgumentException e)
    {
        logException(request, new RakamException(e.getMessage(), HttpResponseStatus.BAD_REQUEST));
    }

    public static void logQueryError(String query, QueryError e, Class<? extends QueryExecutor> queryExecutorClass)
    {
        EventBuilder builder = new EventBuilder()
                .withMessage(e.message)
                .withExtra("query", query)
                .withLevel(Event.Level.WARNING)
                .withLogger(QueryError.class.getName())
                .withTag("executor", queryExecutorClass.getName());

        if (TAGS != null) {
            for (Map.Entry<String, String> entry : TAGS.entrySet()) {
                builder.withTag(entry.getKey(), entry.getValue());
            }
        }

        if (RELEASE != null) {
            builder.withRelease(RELEASE);
        }

        // TODO log errors to Rakam
    }
}
