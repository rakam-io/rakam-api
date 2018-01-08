package org.rakam.plugin;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.cookie.Cookie;
import org.rakam.collection.Event;
import org.rakam.collection.EventList;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface SyncEventMapper
        extends EventMapper {
    default List<Cookie> map(EventList events, RequestParams requestParams, InetAddress sourceAddress, HttpHeaders responseHeaders) {
        List<Cookie> cookies = null;
        for (Event event : events.events) {
            List<Cookie> map = map(event, requestParams, sourceAddress, responseHeaders);
            if (map != null) {
                if (cookies == null) {
                    cookies = new ArrayList<>();
                }
                cookies.addAll(map);
            }
        }
        return cookies;
    }

    List<Cookie> map(Event event, RequestParams requestParams, InetAddress sourceAddress, HttpHeaders responseHeaders);

    default CompletableFuture<List<Cookie>> mapAsync(Event event, RequestParams requestParams, InetAddress sourceAddress, HttpHeaders responseHeaders) {
        List<Cookie> map = map(event, requestParams, sourceAddress, responseHeaders);
        if (map == null) {
            return COMPLETED_EMPTY_FUTURE;
        }

        return CompletableFuture.completedFuture(map);
    }

}
