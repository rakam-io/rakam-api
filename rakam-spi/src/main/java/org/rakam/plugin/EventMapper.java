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


public interface EventMapper {
    List<Cookie> map(Event event, RequestParams requestParams, InetAddress sourceAddress, HttpHeaders responseHeaders);

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

    default void addFieldDependency(FieldDependencyBuilder builder) {
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