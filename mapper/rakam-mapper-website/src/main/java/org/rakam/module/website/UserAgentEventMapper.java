package org.rakam.module.website;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.cookie.Cookie;
import org.apache.avro.generic.GenericRecord;
import org.rakam.Mapper;
import org.rakam.collection.Event;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.SyncEventMapper;
import org.rakam.plugin.user.ISingleUserBatchOperation;
import org.rakam.plugin.user.UserPropertyMapper;
import org.rakam.server.http.HttpRequestException;
import org.rakam.util.MapProxyGenericRecord;
import ua_parser.CachingParser;
import ua_parser.Client;
import ua_parser.Parser;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;

import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static org.rakam.util.AvroUtil.put;

@Mapper(name = "User Agent Event mapper",
        description = "Parses user agent string and attaches new field related with the user agent of the user")
public class UserAgentEventMapper implements SyncEventMapper, UserPropertyMapper {
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
    public List<Cookie> map(String project, List<? extends ISingleUserBatchOperation> user, RequestParams requestParams, InetAddress sourceAddress) {
        for (ISingleUserBatchOperation data : user) {
            if (data.getSetProperties() != null) {
                mapInternal(requestParams, new MapProxyGenericRecord(data.getSetProperties()),
                        data.getSetProperties().get("_user_agent"));
            }

            if (data.getSetPropertiesOnce() != null) {
                mapInternal(requestParams, new MapProxyGenericRecord(data.getSetPropertiesOnce()),
                        data.getSetPropertiesOnce().get("_user_agent"));
            }
        }
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
                throw new HttpRequestException("Spiders are not allowed in Rakam Analytics.", FORBIDDEN);
            }

            if (properties.get("user_agent_family") == null) {
                put(properties,"_user_agent_family", parsed.userAgent.family);
            }

            if (trackSpiders && parsed.userAgent != null && properties.get("_user_agent_version") == null) {
                try {
                    put(properties,"_user_agent_version", parsed.userAgent.major);
                } catch (NumberFormatException e) {
                }
            }

            if (parsed.device != null && properties.get("_device_family") == null) {
                put(properties,"_device_family", parsed.device.family);
            }

            if (parsed.os != null) {
                if (properties.get("_os") == null) {
                    put(properties, "_os", parsed.os.family);
                }

                if (parsed.os.major != null && properties.get("_os_version") == null) {
                    try {
                        put(properties,"_os_version", parsed.os.major);
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
                new SchemaField("_user_agent_version", FieldType.STRING),
                new SchemaField("_os", FieldType.STRING),
                new SchemaField("_os_version", FieldType.STRING),
                new SchemaField("_device_family", FieldType.STRING)
        ));
    }

}
