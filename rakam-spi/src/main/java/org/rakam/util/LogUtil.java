package org.rakam.util;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.sentry.Sentry;
import io.sentry.event.Event;
import io.sentry.event.EventBuilder;
import io.sentry.event.interfaces.HttpInterface;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.RakamServletWrapper;

public class LogUtil {

    public static void logException(RakamHttpRequest request, RakamException e) {
        Sentry.capture(buildEvent(request)
                .withMessage(e.getErrors().get(0).title)
                .withTag("status", e.getStatusCode().reasonPhrase()).withLevel(Event.Level.WARNING)
                .build());
    }

    public static void logException(RakamHttpRequest request, Throwable e) {
        Sentry.capture(buildEvent(request)
                .withMessage(e.getMessage())
                .withLevel(Event.Level.ERROR));
    }

    private static EventBuilder buildEvent(RakamHttpRequest request) {
        return new EventBuilder()
                .withSentryInterface(new HttpInterface(new RakamServletWrapper(request)))
                .withLogger(RakamException.class.getName())
                .withRelease(RakamClient.RELEASE);
    }

    public static void logException(RakamHttpRequest request, IllegalArgumentException e) {
        logException(request, new RakamException(e.getMessage(), HttpResponseStatus.BAD_REQUEST));
    }
}
