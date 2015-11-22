package org.rakam.plugin;

import io.netty.handler.codec.http.HttpHeaders;

import java.net.InetAddress;
import java.util.Map;

public interface UserPropertyMapper {
    void map(String project, Map<String, Object> properties, HttpHeaders extraProperties, InetAddress sourceAddress);
}

