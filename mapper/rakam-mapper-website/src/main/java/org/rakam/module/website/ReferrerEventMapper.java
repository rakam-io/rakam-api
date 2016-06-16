package org.rakam.module.website;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.snowplowanalytics.refererparser.CorruptYamlException;
import com.snowplowanalytics.refererparser.Parser;
import com.snowplowanalytics.refererparser.Referer;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.cookie.Cookie;
import org.apache.avro.generic.GenericRecord;
import org.rakam.collection.Event;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.user.UserPropertyMapper;
import org.rakam.util.MapProxyGenericRecord;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.util.List;

public class ReferrerEventMapper
        implements EventMapper, UserPropertyMapper
{

    private final Parser parser;

    public ReferrerEventMapper()
    {
        try {
            parser = new Parser();
        }
        catch (IOException | CorruptYamlException e) {
            throw Throwables.propagate(e);
        }
    }

    private void mapInternal(RequestParams extraProperties, Object referrer, Object host, GenericRecord record)
    {
        String hostUrl, referrerUrl;
        if (referrer instanceof Boolean && ((Boolean) referrer).booleanValue()) {
            hostUrl = extraProperties.headers().get("Origin");
            referrerUrl = extraProperties.headers().get("Referer");
        }
        else if (referrer instanceof String) {
            referrerUrl = (String) referrer;
            if (host instanceof String) {
                hostUrl = (String) host;
            }
            else {
                hostUrl = null;
            }
        }
        else {
            return;
        }

        Referer parse;
        if (referrerUrl != null) {
            try {
                parse = parser.parse(referrerUrl, hostUrl);
            }
            catch (URISyntaxException e) {
                return;
            }
            if (parse == null) {
                return;
            }

            if (record.get("_referrer_medium") == null) {
                record.put("_referrer_medium", parse.medium != null ? parse.medium.toString().toLowerCase() : null);
            }
            if (record.get("_referrer_source") == null) {
                record.put("_referrer_source", parse.source);
            }

            if (record.get("_referrer_term") == null) {
                record.put("_referrer_term", parse.term);
            }
        }
    }

    @Override
    public List<Cookie> map(Event event, RequestParams extraProperties, InetAddress sourceAddress, HttpHeaders responseHeaders)
    {
        Object referrer = event.properties().get("_referrer");
        Object host = event.properties().get("_host");
        mapInternal(extraProperties, referrer, host, event.properties());
        return null;
    }

    @Override
    public List<Cookie> map(String project, BatchUserOperation user, RequestParams extraProperties, InetAddress sourceAddress)
    {
        for (BatchUserOperation.Data data : user.data) {
            if (data.setProperties != null) {
                mapInternal(extraProperties, data.setProperties.get("_referrer"), data.setProperties.get("_host"), new MapProxyGenericRecord(data.setProperties));
            }

            if (data.setPropertiesOnce != null) {
                mapInternal(extraProperties, data.setPropertiesOnce.get("_referrer"), data.setPropertiesOnce.get("_host"), new MapProxyGenericRecord(data.setProperties));
            }
        }
        return null;
    }

    @Override
    public void addFieldDependency(FieldDependencyBuilder builder)
    {
        ImmutableList<SchemaField> list = ImmutableList.of(
                new SchemaField("_referrer_medium", FieldType.STRING),
                new SchemaField("_referrer_source", FieldType.STRING),
                new SchemaField("_referrer_term", FieldType.STRING)
        );
        builder.addFields("_referrer", list);
        builder.addFields("_host", list);
    }
}
