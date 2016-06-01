package org.rakam.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.getsentry.raven.DefaultRavenFactory;
import com.getsentry.raven.Raven;
import com.getsentry.raven.RavenFactory;
import com.getsentry.raven.dsn.Dsn;
import com.getsentry.raven.event.Event;
import com.getsentry.raven.event.EventBuilder;
import com.getsentry.raven.event.interfaces.HttpInterface;
import com.getsentry.raven.event.interfaces.SentryInterface;
import com.getsentry.raven.jul.SentryHandler;
import com.getsentry.raven.marshaller.Marshaller;
import com.getsentry.raven.marshaller.json.InterfaceBinding;
import com.getsentry.raven.marshaller.json.JsonMarshaller;
import com.google.common.collect.ImmutableMap;
import org.rakam.report.QueryError;
import org.rakam.report.QueryExecutor;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.RakamServletWrapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.logging.LogManager;
import java.util.stream.Collectors;

public class SentryUtil
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

        RavenFactory.registerFactory(new QueryRavenFactory());
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

    public static void logQueryError(String query, QueryError e, Class<? extends QueryExecutor> queryExecutorClass)
    {
        if (RAVEN == null) {
            return;
        }
        EventBuilder builder = new EventBuilder()
                .withMessage(e.message)
                .withExtra("query", query)
                .withSentryInterface(new QuerySentryInterface(query, queryExecutorClass.getName()))
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

        RAVEN.sendEvent(builder.build());
    }

    private static class QuerySentryInterface
            implements SentryInterface
    {
        private final String query;
        private final String engine;

        public QuerySentryInterface(String query, String engine)
        {
            this.query = query;
            this.engine = engine;
        }

        @Override
        public String getInterfaceName()
        {
            return "sentry.interfaces.Query";
        }

        public String getQuery()
        {
            return query;
        }

        public String getEngine()
        {
            return engine;
        }
    }

    private static class QueryInterfaceInterfaceBinding
            implements InterfaceBinding<QuerySentryInterface>
    {
        @Override
        public void writeInterface(JsonGenerator generator, QuerySentryInterface sentryInterface)
                throws IOException
        {
            generator.writeStartObject();
            generator.writeStringField("query", sentryInterface.getQuery());
            generator.writeStringField("engine", sentryInterface.getEngine());
            generator.writeEndObject();
        }
    }

    private static class QueryRavenFactory
            extends DefaultRavenFactory
    {
        @Override
        public Raven createRavenInstance(Dsn dsn)
        {
            return super.createRavenInstance(dsn);
        }

        @Override
        protected Marshaller createMarshaller(Dsn dsn)
        {
            JsonMarshaller marshaller = (JsonMarshaller) super.createMarshaller(dsn);
            marshaller.addInterfaceBinding(QuerySentryInterface.class, new QueryInterfaceInterfaceBinding());
            return marshaller;
        }
    }
}
