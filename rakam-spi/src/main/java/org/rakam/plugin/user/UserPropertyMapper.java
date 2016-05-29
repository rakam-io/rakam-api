package org.rakam.plugin.user;

import org.rakam.plugin.EventMapper;

import java.net.InetAddress;
import java.util.Map;

public interface UserPropertyMapper {
    void map(String project, Map<String, Object> properties, EventMapper.RequestParams requestParams, InetAddress sourceAddress);
}

