package org.rakam.plugin.geoip;


import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import com.maxmind.geoip.timeZone;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.codehaus.jackson.node.NullNode;
import org.rakam.collection.event.metastore.EventSchemaMetastore;
import org.rakam.model.Event;
import org.rakam.plugin.EventMapper;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static org.apache.avro.Schema.Type.NULL;
import static org.apache.avro.Schema.Type.STRING;
import static org.apache.avro.Schema.create;
import static org.apache.avro.Schema.createUnion;

/**
 * Created by buremba on 26/05/14.
 */
public class GeoIPEventMapper implements EventMapper {
    private final GeoIPModuleConfig config;
    LookupService lookup;
    String[] attributes;
    private EventSchemaMetastore schemaMetastore;

    public GeoIPEventMapper(GeoIPModuleConfig config) throws IOException {
        this.config = checkNotNull(config, "config is null");
        lookup = new LookupService(config.getDatabase(), LookupService.GEOIP_MEMORY_CACHE);
        attributes = Arrays.stream(config.getAttributes().split(","))
                .map(attr -> attr.trim()).toArray(String[]::new);
    }

    @Inject
    public void setSchemaMetastore(EventSchemaMetastore schemaMetastore) {
        this.schemaMetastore = schemaMetastore;
    }

    @Override
    public void map(Event event) {
        GenericData.Record properties = event.properties();
        String IP = (String) properties.get("ip");
        if (IP != null) {
            Location l1;
            try {
                l1 = lookup.getLocation(IP);
            } catch (Exception e) {
                return;
            }

            if(l1 == null) {
                return;
            }

            // TODO: we can compile a lambda that attaches appropriate attributes to events based on config values
            for (String attribute : attributes) {
                switch (attribute) {
                    case "country":
                        properties.put("country", l1.countryName);
                        break;
                    case "countryCode":
                        properties.put("countryCode", l1.countryCode);
                        break;
                    case "region":
                        properties.put("region", l1.region);
                        break;
                    case "city":
                        properties.put("city", l1.city);
                        break;
                    case "latitude":
                        properties.put("latitude", l1.latitude);
                        break;
                    case "longitude":
                        properties.put("longitude", l1.longitude);
                        break;
                    case "timezone":
                        String timezone = timeZone.timeZoneByCountryAndRegion(l1.countryCode, l1.region);
                        properties.put("timezone", timezone);
                        break;
                }
            }
        }
    }

    @Override
    public List<Schema.Field> fields() {
        return Arrays.stream(attributes).map(attr -> {
            Schema type = createUnion(newArrayList(create(NULL), create(STRING)));
            return new Schema.Field(attr, type, null, NullNode.getInstance());
        }).collect(Collectors.toList());
    }

}
