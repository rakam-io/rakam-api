package org.rakam.plugin.geoip;


import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import com.maxmind.geoip.timeZone;
import org.rakam.plugin.EventMapper;

import java.io.IOException;

/**
 * Created by buremba on 26/05/14.
 */
public class GeoIPEventMapper implements EventMapper {
    LookupService lookup;

    @Inject
    public GeoIPEventMapper() throws IOException {
//        lookup = new LookupService(ServiceStarter.conf.getString("geoip_location"), LookupService.GEOIP_MEMORY_CACHE );
    }

    @Override
    public void map(ObjectNode event) {
        String IP = event.get("ip").asText();
        if (IP != null) {
            Location l1 = lookup.getLocation(IP);
            event.put("country", l1.countryName)
            .put("country code", l1.countryCode)
            .put("region", l1.region)
            .put("city", l1.city)
            .put("latitude", l1.latitude)
            .put("longitude", l1.longitude)
            .put("timezone", timeZone.timeZoneByCountryAndRegion(l1.countryCode, l1.region));
        }
    }
}
