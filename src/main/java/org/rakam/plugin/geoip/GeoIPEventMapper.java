package org.rakam.plugin.geoip;


import com.google.inject.Inject;
import com.maxmind.geoip.*;
import org.rakam.ServiceStarter;
import org.rakam.plugin.CollectionMapperPlugin;
import org.vertx.java.core.json.JsonObject;

import java.io.IOException;

/**
 * Created by buremba on 26/05/14.
 */
public class GeoIPEventMapper implements CollectionMapperPlugin {
    LookupService lookup;

    @Inject
    public GeoIPEventMapper() throws IOException {
        lookup = new LookupService(ServiceStarter.conf.getString("geoip_location"), LookupService.GEOIP_MEMORY_CACHE );
    }

    @Override
    public boolean map(JsonObject event) {
         String IP = event.getString("ip");
         if(IP!=null) {
             Location l1 = lookup.getLocation(IP);
             event.putString("country", l1.countryName);
             event.putString("country code", l1.countryCode);
             event.putString("region", l1.region);
             event.putString("city", l1.city);
             event.putNumber("latitude", l1.latitude);
             event.putNumber("longitude", l1.longitude);
             event.putString("timezone", timeZone.timeZoneByCountryAndRegion(l1.countryCode, l1.region));
         }
        return true;
    }
}
