package org.rakam.plugin;

import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.cookie.Cookie;
import org.rakam.collection.Event;
import org.rakam.collection.FieldDependencyBuilder;

import java.net.InetAddress;
import java.util.List;


public interface EventMapper {
    List<Cookie> map(Event event, HttpHeaders extraProperties, InetAddress sourceAddress, DefaultFullHttpResponse response);

    default void addFieldDependency(FieldDependencyBuilder builder) {}
}