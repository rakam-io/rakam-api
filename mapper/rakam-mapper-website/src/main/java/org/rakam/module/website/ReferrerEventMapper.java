package org.rakam.module.website;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.snowplowanalytics.refererparser.CorruptYamlException;
import com.snowplowanalytics.refererparser.Parser;
import com.snowplowanalytics.refererparser.Referer;
import io.airlift.log.Logger;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.cookie.Cookie;
import org.apache.avro.generic.GenericRecord;
import org.rakam.collection.Event;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.user.ISingleUserBatchOperation;
import org.rakam.plugin.user.UserPropertyMapper;
import org.rakam.util.MapProxyGenericRecord;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;

public class ReferrerEventMapper
        implements EventMapper, UserPropertyMapper
{
    private final static Logger LOGGER = Logger.get(ReferrerEventMapper.class);

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
        URI referrerUri;

        if (referrerUrl != null) {
            try {
                referrerUri = new URI(referrerUrl);
            }
            catch (URISyntaxException e) {
                return;
            }

            try {
                parse = parser.parse(referrerUri, hostUrl);
            }
            catch (Exception e) {
                LOGGER.warn(e, "Error while parsing referrer");
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

            if (record.get("_referrer_domain") == null) {
                record.put("_referrer_domain", referrerUri.getHost());
            }

            if (record.get("_referrer_path") == null) {
                record.put("_referrer_path", referrerUri.getPath() +
                        (referrerUri.getQuery() == null ? "" : ("?" + referrerUri.getQuery())));
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
    public List<Cookie> map(String project, List<? extends ISingleUserBatchOperation> user, RequestParams extraProperties, InetAddress sourceAddress)
    {
        for (ISingleUserBatchOperation data : user) {
            if (data.getSetProperties() != null) {
                mapInternal(extraProperties, data.getSetProperties().get("_referrer"),
                        data.getSetProperties().get("_host"),
                        new MapProxyGenericRecord(data.getSetProperties()));
            }

            if (data.getSetPropertiesOnce() != null) {
                mapInternal(extraProperties, data.getSetPropertiesOnce().get("_referrer"),
                        data.getSetPropertiesOnce().get("_host"),
                        new MapProxyGenericRecord(data.getSetProperties()));
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
                new SchemaField("_referrer_term", FieldType.STRING),
                new SchemaField("_referrer_domain", FieldType.STRING),
                new SchemaField("_referrer_path", FieldType.STRING)
        );
        builder.addFields("_referrer", list);
        builder.addFields("_host", list);
    }
}
