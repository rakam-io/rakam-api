package org.rakam.plugin;

import com.google.common.collect.ImmutableList;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.cookie.Cookie;
import org.rakam.collection.Event;
import org.rakam.collection.EventList;
import org.rakam.collection.FieldDependencyBuilder;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface EventMapper {
    CompletableFuture<List<Cookie>> COMPLETED_EMPTY_FUTURE = CompletableFuture.completedFuture(null);

    CompletableFuture<List<Cookie>> mapAsync(Event event, RequestParams requestParams, InetAddress sourceAddress, HttpHeaders responseHeaders);

    default CompletableFuture<List<Cookie>> mapAsync(EventList events, RequestParams requestParams, InetAddress sourceAddress, HttpHeaders responseHeaders) {
        List<Cookie> cookies = new ArrayList<>();
        CompletableFuture[] futures = null;
        int futureIndex = 0;
        for (int i = 0; i < events.events.size(); i++) {
            Event event = events.events.get(i);
            CompletableFuture<List<Cookie>> map = mapAsync(event, requestParams, sourceAddress, responseHeaders);
            if (map == null || map.equals(COMPLETED_EMPTY_FUTURE)) {
                continue;
            }

            CompletableFuture<List<Cookie>> future = map.thenApply(value -> {
                if (value != null) {
                    cookies.addAll(value);
                }
                return cookies;
            });

            if (futures == null) {
                futures = new CompletableFuture[events.events.size() - i];
            }

            futures[futureIndex++] = future;
        }

        if (futures == null) {
            return COMPLETED_EMPTY_FUTURE;
        } else {
            return CompletableFuture.allOf(futures).thenApply(val -> cookies);
        }
    }

    default void addFieldDependency(FieldDependencyBuilder builder) {
    }

    default void init() {
    }

    interface RequestParams {
        RequestParams EMPTY_PARAMS = new RequestParams() {
            @Override
            public Collection<Cookie> cookies() {
                return ImmutableList.of();
            }

            @Override
            public HttpHeaders headers() {
                return HttpHeaders.EMPTY_HEADERS;
            }
        };

        default Collection<Cookie> cookies() {
            return ImmutableList.of();
        }

        HttpHeaders headers();
    }
}