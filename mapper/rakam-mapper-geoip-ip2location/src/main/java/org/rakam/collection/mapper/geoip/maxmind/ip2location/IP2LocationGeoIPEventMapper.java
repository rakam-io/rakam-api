package org.rakam.collection.mapper.geoip.maxmind.ip2location;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
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
import org.rakam.util.MapProxyGenericRecord;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.stream.Collectors;

import static org.rakam.collection.FieldType.STRING;
import static org.rakam.collection.mapper.geoip.maxmind.ip2location.IP2LocationGeoIPModule.downloadOrGetFile;
import static org.rakam.util.AvroUtil.put;

@Mapper(name = "IP2Location Event mapper", description = "Looks up geolocation data from _ip field using IP2Location and attaches geo-related attributed")
public class IP2LocationGeoIPEventMapper
        implements SyncEventMapper, UserPropertyMapper {
    private static final Logger LOGGER = Logger.get(IP2LocationGeoIPEventMapper.class);
    private final static List<String> CITY_DATABASE_ATTRIBUTES = ImmutableList
            .of("city", "region", "country_code", "latitude", "longitude");

    private final IPReader lookup;

    public IP2LocationGeoIPEventMapper(GeoIPModuleConfig config)
            throws IOException {
        Preconditions.checkNotNull(config, "config is null");

        lookup = getReader(config.getDatabaseUrl());
    }

    private static FieldType getType(String attr) {
        switch (attr) {
            case "country_code":
            case "region":
            case "city":
            case "timezone":
                return STRING;
            case "latitude":
            case "longitude":
                return FieldType.DOUBLE;
            default:
                throw new IllegalStateException();
        }
    }

    private IPReader getReader(String url) {
        try {
            FileInputStream cityDatabase = new FileInputStream(downloadOrGetFile(url));
            return IPReader.build(cityDatabase);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<Cookie> map(Event event, RequestParams extraProperties, InetAddress sourceAddress, HttpHeaders responseHeaders) {
        Object ip = event.properties().get("_ip");

        InetAddress addr;
        if ((ip instanceof String)) {
            try {
                // it may be slow because java performs reverse hostname lookup.
                addr = Inet4Address.getByName((String) ip);
            } catch (UnknownHostException e) {
                return null;
            }
        } else if (Boolean.TRUE == ip) {
            addr = sourceAddress;
        } else {
            if (lookup != null) {
                // Cloudflare country code header (Only works when the request passed through CF servers)
                String countryCode = extraProperties.headers().get("HTTP_CF_IPCOUNTRY");
                if (countryCode != null) {
                    put(event.properties(),"_country_code", countryCode);
                }
            }

            return null;
        }

        setGeoFields(event.properties(), addr);
        return null;
    }

    @Override
    public List<Cookie> map(String project, List<? extends ISingleUserBatchOperation> user, RequestParams requestParams, InetAddress sourceAddress) {
        for (ISingleUserBatchOperation data : user) {
            if (data.getSetProperties() != null) {
                mapInternal(project, data.getSetProperties(), sourceAddress);
            }
            if (data.getSetPropertiesOnce() != null) {
                mapInternal(project, data.getSetPropertiesOnce(), sourceAddress);
            }
        }

        return null;
    }

    public void mapInternal(String project, ObjectNode data, InetAddress sourceAddress) {
        Object ip = data.get("_ip");

        if (ip == null) {
            return;
        }

        if ((ip instanceof String)) {
            try {
                // it may be slow because java performs reverse hostname lookup.
                sourceAddress = Inet4Address.getByName((String) ip);
            } catch (UnknownHostException e) {
                return;
            }
        }

        GenericRecord record = new MapProxyGenericRecord(data);
        setGeoFields(record, sourceAddress);
    }

    @Override
    public void addFieldDependency(FieldDependencyBuilder builder) {
        List<SchemaField> fields = CITY_DATABASE_ATTRIBUTES.stream()
                .map(attr -> new SchemaField("_" + attr, getType(attr)))
                .collect(Collectors.toList());

        builder.addFields("_ip", fields);
    }

    private void setGeoFields(GenericRecord record, InetAddress address) {
        GeoLocation city = lookup.lookup(address);
        put(record,"_country_code", city.country);
        put(record,"_region", city.stateProv);
        put(record,"_city", city.city);
        put(record,"_latitude", city.coordination.latitude);
        put(record,"_longitude", city.coordination.longitude);
    }
}
