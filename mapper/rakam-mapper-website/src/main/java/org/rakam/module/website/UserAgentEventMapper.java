package org.rakam.module.website;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.rakam.collection.Event;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.FieldDependencyBuilder;
import org.rakam.plugin.EventMapper;
import ua_parser.Client;
import ua_parser.Parser;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;

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
    public void map(Event event, Iterable<Map.Entry<String, String>> extraProperties, InetAddress sourceAddress) {
        Object user_agent = event.properties().get("user_agent");
        if(user_agent != null) {
            Client parsed = uaParser.parse((String) user_agent);
            event.properties().put("user_agent_family", parsed.userAgent.family);
            event.properties().put("user_agent_version", parsed.userAgent.minor + " / " + parsed.userAgent.major);
            event.properties().put("os", parsed.os.family);
            event.properties().put("os_version", parsed.os.minor + " / " + parsed.os.major);
            event.properties().put("device_family", parsed.device.family);

            event.properties().put("user_agent", null);
        }
    }

    @Override
    public void addFieldDependency(FieldDependencyBuilder builder) {
        builder.addFields("user_agent", ImmutableList.of(
                new SchemaField("user_agent_family", FieldType.STRING, true),
                new SchemaField("user_agent_version", FieldType.STRING, true),
                new SchemaField("os", FieldType.STRING, true),
                new SchemaField("os_version", FieldType.STRING, true),
                new SchemaField("device_family", FieldType.STRING, true)
        ));
    }
}
