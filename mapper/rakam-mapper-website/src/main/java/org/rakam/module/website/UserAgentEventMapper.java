package org.rakam.module.website;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.cookie.Cookie;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.rakam.collection.Event;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.user.UserPropertyMapper;
import org.rakam.server.http.HttpRequestException;
import ua_parser.CachingParser;
import ua_parser.Client;
import ua_parser.Parser;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;

public class UserAgentEventMapper implements EventMapper, UserPropertyMapper {
    private final Parser uaParser;
    private final boolean trackSpiders;

    @Inject
    public UserAgentEventMapper(WebsiteMapperConfig config) {
        try {
            uaParser = new CachingParser();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        this.trackSpiders = config.getTrackSpiders();
    }

    @Override
    public List<Cookie> map(String project, Map<String, Object> properties, RequestParams requestParams, InetAddress sourceAddress) {
        mapInternal(requestParams, new MapProxyGenericRecord(properties), properties.get("_user_agent"));
        return null;
    }

    @Override
    public List<Cookie> map(Event event, RequestParams extraProperties, InetAddress sourceAddress, HttpHeaders responseHeaders) {
        GenericRecord properties = event.properties();
        mapInternal(extraProperties, properties, properties.get("_user_agent"));
        return null;
    }

    private void mapInternal(RequestParams extraProperties, GenericRecord properties, Object agent) {
        String userAgent;
        if (agent instanceof Boolean && ((Boolean) agent).booleanValue()) {
            userAgent = extraProperties.headers().get("User-Agent");
        } else if (agent instanceof String) {
            userAgent = (String) agent;
        } else {
            userAgent = null;
        }

        if (userAgent != null) {
            Client parsed;
            try {
                parsed = uaParser.parse(userAgent);
            } catch (Exception e) {
                return;
            }

            if (parsed.device != null && "Spider".equals(parsed.device.family)) {
                // A bit SEO wouldn't hurt.
                throw new HttpRequestException("Spiders are not allowed in Rakam Analytics.", HttpResponseStatus.FORBIDDEN);
            }

            if (properties.get("user_agent_family") == null) {
                properties.put("_user_agent_family", parsed.userAgent.family);
            }

            if (trackSpiders && parsed.userAgent != null && properties.get("_user_agent_version") == null) {
                try {
                    properties.put("_user_agent_version", Long.parseLong(parsed.userAgent.major));
                } catch (NumberFormatException e) {
                }
            }

            if (parsed.device != null && properties.get("_device_family") == null) {
                properties.put("_device_family", parsed.device.family);
            }

            if (parsed.os != null) {
                if (properties.get("_os") == null) {
                    properties.put("_os", parsed.os.family);
                }

                if (parsed.os.major != null && properties.get("_os_version") == null) {
                    try {
                        properties.put("_os_version", Long.parseLong(parsed.os.major));
                    } catch (Exception e) {
                    }
                }
            }
        }
    }

    @Override
    public void addFieldDependency(FieldDependencyBuilder builder) {
        builder.addFields("_user_agent", ImmutableList.of(
                new SchemaField("_user_agent_family", FieldType.STRING),
                new SchemaField("_user_agent_version", FieldType.LONG),
                new SchemaField("_os", FieldType.STRING),
                new SchemaField("_os_version", FieldType.LONG),
                new SchemaField("_device_family", FieldType.STRING)
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
