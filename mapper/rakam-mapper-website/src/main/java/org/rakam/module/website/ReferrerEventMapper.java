package org.rakam.module.website;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.snowplowanalytics.refererparser.CorruptYamlException;
import com.snowplowanalytics.refererparser.Parser;
import com.snowplowanalytics.refererparser.Referer;
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
import org.rakam.plugin.user.UserPropertyMapper;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

public class ReferrerEventMapper implements EventMapper, UserPropertyMapper {

    private final Parser parser;

    public ReferrerEventMapper() {
        try {
            parser = new Parser();
        } catch (IOException | CorruptYamlException e) {
            throw Throwables.propagate(e);
        }
    }

    private void mapInternal(HttpHeaders extraProperties, Object referrer, Object host, GenericRecord record) {
        String hostUrl, referrerUrl;
        if (referrer instanceof Boolean && ((Boolean) referrer).booleanValue()) {
            hostUrl = extraProperties.get("Origin");
            referrerUrl = extraProperties.get("Referer");
        } else if (referrer instanceof String) {
            referrerUrl = (String) referrer;
            if (host instanceof String) {
                hostUrl = (String) host;
            } else {
                hostUrl = null;
            }
        } else {
            return;
        }

        Referer parse;
        if (referrerUrl != null) {
            try {
                parse = parser.parse(referrerUrl, hostUrl);
            } catch (URISyntaxException e) {
                return;
            }
            if (parse == null) {
                return;
            }

            if (record.get("referrer_medium") == null) {
                record.put("referrer_medium", parse.medium != null ? parse.medium.toString() : null);
            }
            if (record.get("referrer_source") == null) {
                record.put("referrer_source", parse.source);
            }

            if (record.get("referrer_term") == null) {
                record.put("referrer_term", parse.term);
            }
        }
    }

    @Override
    public List<Cookie> map(Event event, HttpHeaders extraProperties, InetAddress sourceAddress, DefaultFullHttpResponse response) {
        Object referrer = event.properties().get("_referrer");
        Object host = event.properties().get("_host");
        mapInternal(extraProperties, referrer, host, event.properties());
        return null;
    }

    @Override
    public void map(String project, Map<String, Object> properties, HttpHeaders extraProperties, InetAddress sourceAddress) {
        mapInternal(extraProperties, properties.get("_referrer"), properties.get("_host"), new MapProxyGenericRecord(properties));
    }

    @Override
    public void addFieldDependency(FieldDependencyBuilder builder) {
        ImmutableList<SchemaField> list = ImmutableList.of(
                new SchemaField("referrer_medium", FieldType.STRING),
                new SchemaField("referrer_source", FieldType.STRING),
                new SchemaField("referrer_term", FieldType.STRING)
        );
        builder.addFields("_referrer", list);
        builder.addFields("_host", list);
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
