package org.rakam.collection.mapper.geoip;


import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.Country;
import org.apache.avro.generic.GenericRecord;
import org.rakam.collection.Event;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.FieldDependencyBuilder;
import org.rakam.plugin.EventMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by buremba on 26/05/14.
 */
public class GeoIPEventMapper implements EventMapper {
    final static Logger LOGGER = LoggerFactory.getLogger(GeoIPEventMapper.class);

    private final static List<String> ATTRIBUTES = ImmutableList.of("country","countryCode","region","city","latitude","longitude","timezone");
    DatabaseReader lookup;
    String[] attributes;

    public GeoIPEventMapper(GeoIPModuleConfig config) throws IOException {
        Preconditions.checkNotNull(config, "config is null");
        InputStream database;
        if(config.getDatabase() != null) {
            database = new FileInputStream(config.getDatabase());
        }else {
            URL resource = getClass().getClassLoader().getResource("data/GeoLite2-Country.mmdb");
            if(resource == null) {
                throw new IllegalStateException("GeoIP module is enabled but database location is not set. Please set plugin.geoip.database.");
            }
            database = resource.openStream();
        }

        lookup = new DatabaseReader.Builder(database).build();
        if(config.getAttributes() != null) {
            for (String attr : config.getAttributes()) {
                if(!ATTRIBUTES.contains(attr)) {
                    throw new IllegalArgumentException("Attribute "+attr+" is not exist. Available attributes: " +
                            Joiner.on(", ").join(ATTRIBUTES));
                }
            }
            attributes = config.getAttributes().stream().toArray(String[]::new);
        } else {
            attributes = ATTRIBUTES.toArray(new String[ATTRIBUTES.size()]);
        }
    }

    @Override
    public void map(Event event) {
        GenericRecord properties = event.properties();
        Object IP = properties.get("ip");
        InetAddress ipAddress;
        try {
            if(!(IP instanceof String)) {
                return;
            }
            ipAddress = InetAddress.getByName((String) IP);
            if(ipAddress == null) {
                return;
            }
        } catch (UnknownHostException e) {
            return;
        }

        // TODO: lazy initialization
        CityResponse response;
        try {
            response = lookup.city(ipAddress);
        } catch (IOException e) {
            LOGGER.error("Error while search for location information. ", e);
            return;
        } catch (GeoIp2Exception e) {
            return;
        }

        Country country = response.getCountry();

        for (String attribute : attributes) {
            switch (attribute) {
                case "country":
                    properties.put("country", country.getName());
                    break;
                case "countryCode":
                    properties.put("countryCode", country.getIsoCode());
                    break;
                case "region":
                    properties.put("region", response.getContinent());
                    break;
                case "city":
                    properties.put("city", response.getCity().getName());
                    break;
                case "latitude":
                    properties.put("latitude", response.getLocation().getLatitude());
                    break;
                case "longitude":
                    properties.put("longitude", response.getLocation().getLongitude());
                    break;
                case "timezone":
                    properties.put("timezone", response.getLocation().getTimeZone());
                    break;
            }
        }
    }

    @Override
    public void addFieldDependency(FieldDependencyBuilder builder) {
        builder.addFields("ip", Arrays.stream(attributes)
                .map(attr -> new SchemaField(attr, FieldType.STRING, true))
                .collect(Collectors.toList()));
    }


}
