package org.rakam.plugin;

import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.cookie.Cookie;
import org.rakam.collection.Event;
import org.rakam.collection.event.FieldDependencyBuilder;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;


public interface EventMapper {
    List<Cookie> map(Event event, Iterable<Map.Entry<String, String>> extraProperties, InetAddress sourceAddress, DefaultFullHttpResponse response);
    default void addFieldDependency(FieldDependencyBuilder builder) {}
}