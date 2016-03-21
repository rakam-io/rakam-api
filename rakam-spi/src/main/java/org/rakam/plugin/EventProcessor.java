package org.rakam.plugin;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.cookie.Cookie;
import org.rakam.collection.Event;

import java.net.InetAddress;
import java.util.List;

public interface EventProcessor {
    List<Cookie> map(Event event, HttpHeaders requestHeaders, InetAddress sourceAddress, HttpHeaders responseHeaders);
}
