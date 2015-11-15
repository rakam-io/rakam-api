package org.rakam.module.website;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.cookie.Cookie;
import org.rakam.collection.Event;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.FieldDependencyBuilder;
import org.rakam.plugin.EventMapper;
import ua_parser.Client;
import ua_parser.Parser;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;

public class UserAgentEventMapper implements EventMapper {
    private final Parser uaParser;

    public UserAgentEventMapper() {
        try {
            uaParser = new Parser();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<Cookie> map(Event event, HttpHeaders extraProperties, InetAddress sourceAddress, DefaultFullHttpResponse response) {
        Object agent = event.properties().get("_user_agent");

        String userAgent;
        if(agent instanceof Boolean) {
            Boolean user_agent = (Boolean) event.properties().get("_user_agent");

            if (user_agent == null || !user_agent.booleanValue()) {
                return null;
            }

            userAgent = extraProperties.get("User-Agent");
        } else {
            userAgent = (String) agent;
        }

        if(userAgent != null) {
            Client parsed = uaParser.parse(userAgent);
            event.properties().put("user_agent_family", parsed.userAgent.family);
            Long major1;
            try {
                major1 = Long.parseLong(parsed.userAgent.major);
            } catch (Exception e) {
                major1 = null;
            }
            event.properties().put("user_agent_version", major1);
            event.properties().put("os", parsed.os.family);
            Long major;
            try {
                major = Long.parseLong(parsed.os.major);
            } catch (Exception e) {
                major = null;
            }
            event.properties().put("os_version", major);
            event.properties().put("device_family", parsed.device.family);
        }
        return null;
    }

    @Override
    public void addFieldDependency(FieldDependencyBuilder builder) {
        builder.addFields("_user_agent", ImmutableList.of(
                new SchemaField("user_agent_family", FieldType.STRING, true),
                new SchemaField("user_agent_version", FieldType.LONG, true),
                new SchemaField("os", FieldType.STRING, true),
                new SchemaField("os_version", FieldType.LONG, true),
                new SchemaField("device_family", FieldType.STRING, true)
        ));
    }
}
