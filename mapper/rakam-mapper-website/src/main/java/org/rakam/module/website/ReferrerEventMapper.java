package org.rakam.module.website;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.snowplowanalytics.refererparser.CorruptYamlException;
import com.snowplowanalytics.refererparser.Parser;
import com.snowplowanalytics.refererparser.Referer;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.cookie.Cookie;
import org.rakam.collection.Event;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.FieldDependencyBuilder;
import org.rakam.plugin.EventMapper;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.util.List;

public class ReferrerEventMapper implements EventMapper {

    private final Parser parser;

    public ReferrerEventMapper() {
        try {
            parser = new Parser();
        }  catch (IOException | CorruptYamlException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<Cookie> map(Event event, HttpHeaders extraProperties, InetAddress sourceAddress, DefaultFullHttpResponse response) {
        Object referrer = event.properties().get("_referrer");
        Object host = event.properties().get("_host");

        String hostUrl, referrerUrl;
        if(referrer instanceof Boolean && ((Boolean) referrer).booleanValue()) {
            hostUrl = extraProperties.get("Origin");
            referrerUrl = extraProperties.get("Referer");
        } else
        if(referrer instanceof String) {
            referrerUrl = (String) referrer;
            if(host instanceof String) {
                hostUrl = (String) host;
            } else {
                hostUrl = null;
            }
        } else {
            return null;
        }

        Referer parse;
        if(referrerUrl != null) {
            try {
               parse = parser.parse(referrerUrl, hostUrl);
            } catch (URISyntaxException e) {
                return null;
            }
            if(parse == null) {
                return null;
            }
            event.properties().put("referrer_medium", parse.medium !=null ? parse.medium.toString() : null);
            event.properties().put("referrer_source", parse.source);
            event.properties().put("referrer_term", parse.term);
        }
        return null;
    }

    @Override
    public void addFieldDependency(FieldDependencyBuilder builder) {
        builder.addFields("_referrer", ImmutableList.of(
                new SchemaField("referrer_medium", FieldType.STRING, true),
                new SchemaField("referrer_source", FieldType.STRING, true),
                new SchemaField("referrer_term", FieldType.STRING, true)
        ));
    }
}
