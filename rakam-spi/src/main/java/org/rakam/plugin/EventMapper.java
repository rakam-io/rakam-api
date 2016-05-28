package org.rakam.plugin;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.cookie.Cookie;
import org.rakam.collection.Event;
import org.rakam.collection.EventList;
import org.rakam.collection.FieldDependencyBuilder;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;


public interface EventMapper {
    List<Cookie> map(Event event, HttpHeaders requestHeaders, InetAddress sourceAddress, HttpHeaders responseHeaders);

    default List<Cookie> map(EventList events, HttpHeaders extraProperties, InetAddress sourceAddress, HttpHeaders responseHeaders) {
        List cookies = null;
        for (Event event : events.events) {
            List<Cookie> map = map(event, extraProperties, sourceAddress, responseHeaders);
            if (map != null) {
                if (cookies == null) {
                    cookies = new ArrayList<>();
                }
                cookies.add(map);
            }
        }
        return cookies;
    }

    default void addFieldDependency(FieldDependencyBuilder builder) {
    }
}