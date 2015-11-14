package org.rakam.collection.mapper.geoip;


import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.maxmind.db.Reader;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.ConnectionTypeResponse;
import com.maxmind.geoip2.model.IspResponse;
import io.airlift.log.Logger;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.cookie.Cookie;
import org.apache.avro.generic.GenericRecord;
import org.rakam.collection.Event;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.FieldDependencyBuilder;
import org.rakam.plugin.EventMapper;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.rakam.collection.mapper.geoip.GeoIPModule.downloadOrGetFile;


public class GeoIPEventMapper implements EventMapper {
    final static Logger LOGGER = Logger.get(GeoIPEventMapper.class);
    final static String ERROR_MESSAGE = "You need to set %s config in order to have '%s' field.";

    private final static List<String> CITY_DATABASE_ATTRIBUTES = ImmutableList
            .of("city", "city_code", "region", "city", "latitude", "longitude", "timezone");

    private final String[] attributes;
    private final GeoIPModuleConfig config;
    private final DatabaseReader connectionTypeLookup;
    private final DatabaseReader ispLookup;
    private final DatabaseReader cityLookup;

    public GeoIPEventMapper(GeoIPModuleConfig config) throws IOException {
        Preconditions.checkNotNull(config, "config is null");
        this.config = config;

        DatabaseReader connectionTypeLookup = null, ispLookup = null, cityLookup = null;
        if (config.getAttributes() != null) {
            for (String attr : config.getAttributes()) {
                if (CITY_DATABASE_ATTRIBUTES.contains(attr)) {
                    if (config.getDatabaseUrl() == null) {
                        throw new IllegalStateException(String.format(ERROR_MESSAGE, "plugin.geoip.database.url", attr));
                    }
                    if (cityLookup == null) {
                        cityLookup = getLookup(attr);
                    }
                    continue;
                } else if ("isp" .equals(attr)) {
                    if (config.getIspDatabaseUrl() == null) {
                        throw new IllegalStateException(String.format(ERROR_MESSAGE, "plugin.geoip.isp-database.url", attr));
                    }
                    if (ispLookup == null) ispLookup = getLookup(attr);
                    continue;
                } else if ("connection_type" .equals(attr)) {
                    if (config.getConnectionTypeDatabaseUrl() == null) {
                        throw new IllegalStateException(String.format(ERROR_MESSAGE, "plugin.geoip.connection-type-database.url", attr));
                    }
                    if (connectionTypeLookup == null) connectionTypeLookup = getLookup(attr);
                    continue;
                }
                throw new IllegalArgumentException("Attribute " + attr + " is not valid. Available attributes: " +
                        Joiner.on(", ").join(CITY_DATABASE_ATTRIBUTES));
            }
            attributes = config.getAttributes().stream().toArray(String[]::new);
        } else {
            if (config.getDatabaseUrl() != null) {
                cityLookup = getLookup(config.getDatabaseUrl());
                attributes = CITY_DATABASE_ATTRIBUTES.stream().toArray(String[]::new);
            } else {
                attributes = null;
            }

            if (config.getIspDatabaseUrl() != null) {
                ispLookup = getLookup(config.getIspDatabaseUrl());
            }
            if (config.getConnectionTypeDatabaseUrl() != null) {
                connectionTypeLookup = getLookup(config.getConnectionTypeDatabaseUrl());
            }
        }

        this.cityLookup = cityLookup;
        this.ispLookup = ispLookup;
        this.connectionTypeLookup = connectionTypeLookup;
    }

    private DatabaseReader getLookup(String url) {
        try {
            FileInputStream cityDatabase = new FileInputStream(downloadOrGetFile(url));
            return new DatabaseReader.Builder(cityDatabase).fileMode(Reader.FileMode.MEMORY).build();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<Cookie> map(Event event, Iterable<Map.Entry<String, String>> extraProperties, InetAddress sourceAddress, DefaultFullHttpResponse response) {
        Object ip = event.properties().get("_ip");

        if((ip instanceof String)) {
            try {
                // it may be slow because java performs reverse hostname lookup.
                sourceAddress = Inet4Address.getByName((String) ip);
            } catch (UnknownHostException e) {
                return null;
            }
        }

        if (connectionTypeLookup != null) {
            setConnectionType(sourceAddress, event.properties());
        }

        if (ispLookup != null) {
            setIsp(sourceAddress, event.properties());
        }

        if (cityLookup != null) {
            setGeoFields(sourceAddress, event.properties());
        }

        return null;
    }

    @Override
    public void addFieldDependency(FieldDependencyBuilder builder) {
        List<SchemaField> fields = Arrays.stream(attributes)
                .map(attr -> new SchemaField(attr, getType(attr), true))
                .collect(Collectors.toList());

        if(ispLookup != null) {
            fields.add(new SchemaField("isp", FieldType.STRING, true));
        }

        if(connectionTypeLookup != null) {
            fields.add(new SchemaField("connection_type", FieldType.STRING, true));
        }

        builder.addFields("_ip", fields);
    }

    private static FieldType getType(String attr) {
        switch (attr) {
            case "country":
            case "city_code":
            case "region":
            case "city":
            case "timezone":
            case "isp":
                return FieldType.STRING;
            case "latitude":
            case "longitude":
                return FieldType.DOUBLE;
            default:
                throw new IllegalStateException();
        }
    }

    private void setConnectionType(InetAddress address, GenericRecord properties) {
        ConnectionTypeResponse connectionType;
        try {
            connectionType = connectionTypeLookup.connectionType(address);
        } catch (AddressNotFoundException e) {
            return;
        } catch (Exception e) {
            LOGGER.error(e, "Error while search for location information. ");
            return;
        }

        properties.put("connection_type", connectionType.getConnectionType().name());
    }

    private void setIsp(InetAddress address, GenericRecord properties) {
        IspResponse isp;
        try {
            isp = ispLookup.isp(address);
        } catch (AddressNotFoundException e) {
            return;
        } catch (Exception e) {
            LOGGER.error(e, "Error while search for location information. ");
            return;
        }

        properties.put("isp", isp.getIsp());
    }

    private void setGeoFields(InetAddress address, GenericRecord properties) {
        CityResponse city;

        try {
            city = cityLookup.city(address);
        } catch (AddressNotFoundException e) {
            return;
        } catch (Exception e) {
            LOGGER.error(e, "Error while search for location information. ");
            return;
        }

        for (String attribute : attributes) {
            switch (attribute) {
                case "country":
                    properties.put("country", city.getCountry().getName());
                    break;
                case "country_code":
                    properties.put("country_code", city.getCountry().getIsoCode());
                    break;
                case "region":
                    properties.put("region", city.getContinent().getName());
                    break;
                case "city":
                    properties.put("city", city.getCity().getName());
                    break;
                case "latitude":
                    properties.put("latitude", city.getLocation().getLatitude());
                    break;
                case "longitude":
                    properties.put("longitude", city.getLocation().getLongitude());
                    break;
                case "timezone":
                    properties.put("timezone", city.getLocation().getTimeZone());
                    break;
            }
        }
    }
}
