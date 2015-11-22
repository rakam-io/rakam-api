package org.rakam.module.website;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.cookie.Cookie;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.rakam.collection.Event;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.FieldDependencyBuilder;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.UserPropertyMapper;
import ua_parser.Client;
import ua_parser.Parser;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;

public class UserAgentEventMapper implements EventMapper, UserPropertyMapper {
    private final Parser uaParser;

    public UserAgentEventMapper() {
        try {
            uaParser = new Parser();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void map(String project, Map<String, Object> properties, HttpHeaders extraProperties, InetAddress sourceAddress) {
        mapInternal(extraProperties, new MapProxyGenericRecord(properties), properties.get("_user_agent"));
    }

    @Override
    public List<Cookie> map(Event event, HttpHeaders extraProperties, InetAddress sourceAddress, DefaultFullHttpResponse response) {
        GenericRecord properties = event.properties();
        mapInternal(extraProperties, properties, properties.get("_user_agent"));
        return null;
    }

    private void mapInternal(HttpHeaders extraProperties, GenericRecord properties, Object agent) {
        String userAgent;
        if(agent instanceof Boolean) {
            Boolean user_agent = (Boolean) properties.get("_user_agent");

            if (user_agent == null || !user_agent.booleanValue()) {
                return;
            }

            userAgent = extraProperties.get("User-Agent");
        } else {
            userAgent = (String) agent;
        }

        if(userAgent != null) {
            Client parsed = uaParser.parse(userAgent);
            properties.put("user_agent_family", parsed.userAgent.family);
            Long major1;
            try {
                major1 = Long.parseLong(parsed.userAgent.major);
            } catch (Exception e) {
                major1 = null;
            }
            properties.put("user_agent_version", major1);
            properties.put("os", parsed.os.family);
            Long major;
            try {
                major = Long.parseLong(parsed.os.major);
            } catch (Exception e) {
                major = null;
            }
            properties.put("os_version", major);
            properties.put("device_family", parsed.device.family);
        }
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

    private static class MapProxyGenericRecord implements GenericRecord {

        private final Map<String, Object> properties;

        public MapProxyGenericRecord(Map<String, Object> properties) {
            this.properties = properties;
        }

        @Override
        public Schema getSchema() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void put(int i, Object v) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object get(int i) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void put(String key, Object v) {
            properties.put(key, v);
        }

        @Override
        public Object get(String key) {
            return properties.get(key);
        }
    }
}
