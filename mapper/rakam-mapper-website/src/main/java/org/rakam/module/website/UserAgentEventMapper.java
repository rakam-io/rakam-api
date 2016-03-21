package org.rakam.module.website;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.cookie.Cookie;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.rakam.collection.Event;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.user.UserPropertyMapper;
import ua_parser.CachingParser;
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
            uaParser = new CachingParser();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void map(String project, Map<String, Object> properties, HttpHeaders extraProperties, InetAddress sourceAddress) {
        mapInternal(extraProperties, new MapProxyGenericRecord(properties), properties.get("_user_agent"));
    }

    @Override
    public List<Cookie> map(Event event, HttpHeaders extraProperties, InetAddress sourceAddress) {
        GenericRecord properties = event.properties();
        mapInternal(extraProperties, properties, properties.get("_user_agent"));
        return null;
    }

    private void mapInternal(HttpHeaders extraProperties, GenericRecord properties, Object agent) {
        String userAgent;
        if(agent instanceof Boolean && ((Boolean) agent).booleanValue()) {
            userAgent = extraProperties.get("User-Agent");
        } else
        if(agent instanceof String){
            userAgent = (String) agent;
        } else {
            userAgent = null;
        }

        if(userAgent != null) {
            Client parsed;
            try {
                parsed = uaParser.parse(userAgent);
            } catch (Exception e) {
                return;
            }

            if(properties.get("user_agent_family") == null) {
                properties.put("user_agent_family", parsed.userAgent.family);
            }

            if(parsed.userAgent != null && properties.get("user_agent_version") == null) {
                try {
                    properties.put("user_agent_version", Long.parseLong(parsed.userAgent.major));
                } catch (NumberFormatException e) {}
            }

            if (parsed.device != null && properties.get("device_family") == null) {
                properties.put("device_family", parsed.device.family);
            }

            if(parsed.os != null) {
                if(properties.get("os") == null) {
                    properties.put("os", parsed.os.family);
                }

                if(parsed.os.major != null && properties.get("os_version") == null) {
                    try {
                        properties.put("os_version", Long.parseLong(parsed.os.major));
                    } catch (Exception e) {
                    }
                }
            }
        }
    }

    @Override
    public void addFieldDependency(FieldDependencyBuilder builder) {
        builder.addFields("_user_agent", ImmutableList.of(
                new SchemaField("user_agent_family", FieldType.STRING),
                new SchemaField("user_agent_version", FieldType.LONG),
                new SchemaField("os", FieldType.STRING),
                new SchemaField("os_version", FieldType.LONG),
                new SchemaField("device_family", FieldType.STRING)
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
