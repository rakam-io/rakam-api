package org.rakam.plugin.user;

import io.netty.handler.codec.http.cookie.Cookie;
import org.rakam.plugin.EventMapper;

import java.net.InetAddress;
import java.util.List;

public interface UserPropertyMapper {
    List<Cookie> map(String project, List<? extends ISingleUserBatchOperation> user, EventMapper.RequestParams requestParams, InetAddress sourceAddress);
}

