package org.rakam.plugin.geoip;


import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import com.maxmind.geoip.timeZone;
import org.apache.avro.generic.GenericData;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.model.Event;
import org.rakam.plugin.EventMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by buremba on 26/05/14.
 */
public class GeoIPEventMapper implements EventMapper {
    LookupService lookup;
    String[] attributes;

    public GeoIPEventMapper(GeoIPModuleConfig config) throws IOException {
        checkNotNull(config, "config is null");
        lookup = new LookupService(config.getDatabase(), LookupService.GEOIP_MEMORY_CACHE);
        attributes = Arrays.stream(config.getAttributes().split(","))
                .map(attr -> attr.trim()).toArray(String[]::new);
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
    public List<SchemaField> fields() {
        return Arrays.stream(attributes)
                .map(attr -> new SchemaField(attr, FieldType.STRING, true)).collect(Collectors.toList());
    }

    @Override
    public void addedFields(List<SchemaField> existingFields, List<SchemaField> newFields) {
        if(existingFields.stream().anyMatch(field -> field.getName().equals("ip"))) {
            Arrays.stream(attributes)
                    .filter(attr -> existingFields.stream().anyMatch(field -> field.getName().equals(attr)))
                    .forEach(attr -> {
                        Optional<SchemaField> any = newFields.stream().filter(field -> field.getName().equals(attr)).findAny();
                        if(any.isPresent()) {
                            if(any.get().getType() != FieldType.STRING) {
                                newFields.remove(any.get());
                            } else {
                                return;
                            }
                        }

                        newFields.add(new SchemaField(attr, FieldType.STRING, true));
                    });
        }
    }


}
