package org.rakam.collection.mapper.geoip.maxmind.ip2location;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class GeoLocation {
    private static final Map<Coordination, GeoLocation> GEO_LOCATION_MAP = new ConcurrentHashMap<>();

    public final String country;
    public final String stateProv;
    public final String city;
    public final Coordination coordination;

    private GeoLocation(String country, String stateProv, String city, Coordination coordination) {
        this.country = country;
        this.stateProv = stateProv;
        this.city = city;
        this.coordination = coordination;
    }

    public static GeoLocation of(String country, String stateProv, String city, Coordination coordination) {
        if (GEO_LOCATION_MAP.containsKey(coordination)) {
            return GEO_LOCATION_MAP.get(coordination);
        }
        GeoLocation newLocation = new GeoLocation(country, stateProv, city, coordination);
        GEO_LOCATION_MAP.put(coordination, newLocation);

        return newLocation;
    }
}