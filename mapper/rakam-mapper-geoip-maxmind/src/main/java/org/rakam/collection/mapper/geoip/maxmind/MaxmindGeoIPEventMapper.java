package org.rakam.collection.mapper.geoip.maxmind;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.ConnectionTypeResponse;
import com.maxmind.geoip2.model.IspResponse;
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
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.maxmind.db.Reader.FileMode.MEMORY;
import static org.rakam.collection.FieldType.STRING;
import static org.rakam.util.AvroUtil.put;

@Mapper(name = "Maxmind Event mapper", description = "Looks up geolocation data from _ip field using Maxmind and attaches geo-related attributed")
public class MaxmindGeoIPEventMapper
        implements SyncEventMapper, UserPropertyMapper {
    private static final Logger LOGGER = Logger.get(MaxmindGeoIPEventMapper.class);
    private static final String ERROR_MESSAGE = "You need to set %s config in order to have '%s' field.";

    private final static List<String> CITY_DATABASE_ATTRIBUTES = ImmutableList
            .of("city", "region", "country_code", "latitude", "longitude", "timezone");
    private static final String IP_ADDRESS_REGEX = "([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3})";
    private static final String PRIVATE_IP_ADDRESS_REGEX = "(^127\\.0\\.0\\.1)|(^10\\.)|(^172\\.1[6-9]\\.)|(^172\\.2[0-9]\\.)|(^172\\.3[0-1]\\.)|(^192\\.168\\.)";
    private static Pattern IP_ADDRESS_PATTERN = null;
    private static Pattern PRIVATE_IP_ADDRESS_PATTERN = null;
    private final String[] attributes;
    private final DatabaseReader connectionTypeLookup;
    private final DatabaseReader ispLookup;
    private final DatabaseReader cityLookup;
    private final boolean attachIp;

    public MaxmindGeoIPEventMapper(MaxmindGeoIPModuleConfig config)
            throws IOException {
        Preconditions.checkNotNull(config, "config is null");

        DatabaseReader connectionTypeLookup = null, ispLookup = null, cityLookup = null;
        boolean attachIp = false;
        if (config.getAttributes() != null) {
            for (String attr : config.getAttributes()) {
                if (CITY_DATABASE_ATTRIBUTES.contains(attr)) {
                    if (config.getDatabaseUrl() == null) {
                        throw new IllegalStateException(String.format(ERROR_MESSAGE, "plugin.geoip.database.url", attr));
                    }
                    if (cityLookup == null) {
                        cityLookup = getReader(config.getDatabaseUrl());
                    }
                    continue;
                } else if ("isp".equals(attr)) {
                    if (config.getIspDatabaseUrl() == null) {
                        throw new IllegalStateException(String.format(ERROR_MESSAGE, "plugin.geoip.isp-database.url", attr));
                    }
                    if (ispLookup == null) {
                        ispLookup = getReader(config.getIspDatabaseUrl());
                    }
                    continue;
                } else if ("connection_type".equals(attr)) {
                    if (config.getConnectionTypeDatabaseUrl() == null) {
                        throw new IllegalStateException(String.format(ERROR_MESSAGE, "plugin.geoip.connection-type-database.url", attr));
                    }
                    if (connectionTypeLookup == null) {
                        connectionTypeLookup = getReader(config.getConnectionTypeDatabaseUrl());
                    }
                    continue;
                } else if ("_ip".equals(attr)) {
                    attachIp = true;
                    continue;
                }
                throw new IllegalArgumentException("Attribute " + attr + " is not valid. Available attributes: " +
                        Joiner.on(", ").join(CITY_DATABASE_ATTRIBUTES));
            }
            attributes = config.getAttributes().stream().toArray(String[]::new);
        } else {
            if (config.getDatabaseUrl() != null) {
                cityLookup = getReader(config.getDatabaseUrl());
                attributes = CITY_DATABASE_ATTRIBUTES.stream().toArray(String[]::new);
            } else {
                attributes = null;
            }
            attachIp = true;
        }
        this.attachIp = attachIp;

        if (config.getIspDatabaseUrl() != null) {
            ispLookup = getReader(config.getIspDatabaseUrl());
        }
        if (config.getConnectionTypeDatabaseUrl() != null) {
            connectionTypeLookup = getReader(config.getConnectionTypeDatabaseUrl());
        }

        this.cityLookup = cityLookup;
        this.ispLookup = ispLookup;
        this.connectionTypeLookup = connectionTypeLookup;
    }

    private static FieldType getType(String attr) {
        switch (attr) {
            case "country_code":
            case "region":
            case "city":
            case "timezone":
            case "_ip":
                return STRING;
            case "latitude":
            case "longitude":
                return FieldType.DOUBLE;
            default:
                throw new IllegalStateException();
        }
    }

    private static String findNonPrivateIpAddress(String s) {
        if (IP_ADDRESS_PATTERN == null) {
            IP_ADDRESS_PATTERN = Pattern.compile(IP_ADDRESS_REGEX);
            PRIVATE_IP_ADDRESS_PATTERN = Pattern.compile(PRIVATE_IP_ADDRESS_REGEX);
        }
        Matcher matcher = IP_ADDRESS_PATTERN.matcher(s);
        while (matcher.find()) {
            String group = matcher.group(0);
            if (group != null && !PRIVATE_IP_ADDRESS_PATTERN.matcher(group).find()) {
                return group;
            }
            matcher.region(matcher.end(), s.length());
        }
        return null;
    }

    private DatabaseReader getReader(URL url) {
        try {
            FileInputStream cityDatabase = new FileInputStream(MaxmindGeoIPModule.downloadOrGetFile(url));
            return new DatabaseReader.Builder(cityDatabase).fileMode(MEMORY).build();
        } catch (Exception e) {
            throw new RuntimeException(e);
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
            String forwardedFor = extraProperties.headers().get("X-Forwarded-For");
            if (forwardedFor != null && (forwardedFor = findNonPrivateIpAddress(forwardedFor)) != null) {
                try {
                    // it may be slow because java performs reverse hostname lookup.
                    addr = Inet4Address.getByName(forwardedFor);
                } catch (UnknownHostException e) {
                    return null;
                }
            } else {
                addr = sourceAddress;
            }
        } else {
            if (cityLookup != null) {
                // Cloudflare country code header (Only works when the request passed through CF servers)
                String countryCode = extraProperties.headers().get("HTTP_CF_IPCOUNTRY");
                if (countryCode != null) {
                    put(event.properties(),"_country_code", countryCode);
                }
            }

            return null;
        }

        if (addr == null) {
            return null;
        }

        if (attachIp) {
            put(event.properties(),"__ip", addr.getHostAddress());
        }

        if (connectionTypeLookup != null) {
            setConnectionType(addr, event.properties());
        }

        if (ispLookup != null) {
            setIsp(addr, event.properties());
        }

        if (cityLookup != null) {
            setGeoFields(addr, event.properties());
        }

        return null;
    }

    @Override
    public List<Cookie> map(String project, List<? extends ISingleUserBatchOperation> user, RequestParams requestParams, InetAddress sourceAddress) {
        for (ISingleUserBatchOperation data : user) {
            if (data.getSetProperties() != null) {
                mapInternal(data.getSetProperties(), sourceAddress);
            }
            if (data.getSetPropertiesOnce() != null) {
                mapInternal(data.getSetPropertiesOnce(), sourceAddress);
            }
        }

        return null;
    }

    public void mapInternal(ObjectNode data, InetAddress sourceAddress) {
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

        if (sourceAddress == null) {
            return;
        }

        GenericRecord record = new MapProxyGenericRecord(data);

        if (connectionTypeLookup != null) {
            setConnectionType(sourceAddress, record);
        }

        if (ispLookup != null) {
            setIsp(sourceAddress, record);
        }

        if (cityLookup != null) {
            setGeoFields(sourceAddress, record);
        }
    }

    @Override
    public void addFieldDependency(FieldDependencyBuilder builder) {
        List<SchemaField> fields = Arrays.stream(attributes)
                .map(attr -> new SchemaField("_" + attr, getType(attr)))
                .collect(Collectors.toList());

        if (ispLookup != null) {
            fields.add(new SchemaField("_isp", STRING));
        }

        if (connectionTypeLookup != null) {
            fields.add(new SchemaField("_connection_type", STRING));
        }

        fields.add(new SchemaField("__ip", STRING));

        builder.addFields("_ip", fields);
    }

    private void setConnectionType(InetAddress address, GenericRecord properties) {
        ConnectionTypeResponse connectionType;
        try {
            connectionType = connectionTypeLookup.connectionType(address);
        } catch (AddressNotFoundException e) {
            return;
        } catch (Exception e) {
            LOGGER.error(e, "Error while searching for location information.");
            return;
        }

        ConnectionTypeResponse.ConnectionType connType = connectionType.getConnectionType();
        if (connType != null) {
            put(properties,"_connection_type", connType.name());
        }
    }

    private void setIsp(InetAddress address, GenericRecord properties) {
        IspResponse isp;
        try {
            isp = ispLookup.isp(address);
        } catch (AddressNotFoundException e) {
            return;
        } catch (Exception e) {
            LOGGER.error(e, "Error while searching for location information.");
            return;
        }

        put(properties,"_isp", isp.getIsp());
    }

    private void setGeoFields(InetAddress address, GenericRecord properties) {
        CityResponse city;

        try {
            city = cityLookup.city(address);
        } catch (AddressNotFoundException e) {
            return;
        } catch (Exception e) {
            LOGGER.error(e, "Error while searching for location information.");
            return;
        }

        for (String attribute : attributes) {
            switch (attribute) {
                case "country_code":
                    put(properties,"_country_code", city.getCountry().getIsoCode());
                    break;
                case "region":
                    put(properties,"_region", city.getContinent().getName());
                    break;
                case "city":
                    put(properties,"_city", city.getCity().getName());
                    break;
                case "latitude":
                    put(properties,"_latitude", city.getLocation().getLatitude());
                    break;
                case "longitude":
                    put(properties,"_longitude", city.getLocation().getLongitude());
                    break;
                case "timezone":
                    put(properties,"_timezone", city.getLocation().getTimeZone());
                    break;
            }
        }
    }
}
