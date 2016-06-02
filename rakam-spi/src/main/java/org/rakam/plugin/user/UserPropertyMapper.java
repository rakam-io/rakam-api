package org.rakam.plugin.user;

import io.netty.handler.codec.http.cookie.Cookie;
import org.rakam.plugin.EventMapper;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;

public interface UserPropertyMapper {
    List<Cookie> map(String project, Map<String, Object> properties, EventMapper.RequestParams requestParams, InetAddress sourceAddress);
}

