package org.rakam.importer.mixpanel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.importer.RakamClient;
import org.rakam.plugin.user.User;
import org.rakam.util.JsonHelper;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.common.io.ByteStreams.toByteArray;
import static java.time.format.DateTimeFormatter.ISO_DATE;
import static org.rakam.importer.mixpanel.MixpanelEventImporter.*;

public class MixpanelImporter {
    private final static List<String> BLACKLIST = ImmutableList.of("$lib_version");

    private final static Logger LOGGER = Logger.get(MixpanelImporter.class);

    private final String apiKey;
    private final String secretKey;

    public MixpanelImporter(String apiKey, String secretKey) {
        this.apiKey = apiKey;
        this.secretKey = secretKey;
    }

    private boolean hasAttribute(Map<String, SchemaField> fieldMap, String name) {
        return fieldMap.entrySet().stream().anyMatch(a -> a.getValue().getName().equals(name));
    }

    public List<String> getCollections() {
        Map<String, String> build = ImmutableMap.<String, String>builder()
                .put("type", "general")
                .put("limit", "4000").build();

        String[] events = JsonHelper.read(generateRequestAndParse("events/names", apiKey, secretKey, build), String[].class);
        return Arrays.asList(events);
    }

    public Map<String, SchemaField> mapPeopleFields() {
        byte[] bytes = generateRequestAndParse("engage/properties", apiKey, secretKey, ImmutableMap.of());
        MixpanelPeopleField read = JsonHelper.read(bytes, MixpanelPeopleField.class);
        return read.results.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey(), e ->
                        new SchemaField(convertRakamName(e.getKey()), MixpanelType.fromMixpanelType(e.getValue().type).type, true)));
    }

    private static class MixpanelPeopleField {
        public final Map<String, TopType> results;
        public final String session_id;
        public final String status;

        @JsonCreator
        private MixpanelPeopleField(@JsonProperty("results") Map<String, TopType> results, @JsonProperty("session_id") String session_id, @JsonProperty("status") String status) {
            this.results = results;
            this.session_id = session_id;
            this.status = status;
        }
    }

    public Map<String, Table> mapEventFields() {
        ImmutableMap.Builder<String, List<SchemaField>> builder = ImmutableMap.builder();

        for (String event : getCollections()) {
            Map<String, TopType> read;
            try {
                read = JsonHelper.read(new String(toByteArray(generateRequest("events/properties/toptypes", apiKey, secretKey,
                        ImmutableMap.of(
                                "event", event,
                                "type", "general",
                                "limit", "500"))), "UTF-8"), new TypeReference<Map<String, TopType>>() {
                });
            } catch (IOException e) {
                LOGGER.error(e, String.format("Error while reading event '%s'", event));
                continue;
            }

            List<SchemaField> types = read.entrySet().stream()
                    .map(e -> new SchemaField(e.getKey(), MixpanelType.fromMixpanelType(e.getValue().type).type, true))
                    .collect(Collectors.toList());
            builder.put(event, types);
        }

        ImmutableMap<String, List<SchemaField>> mixpanelFields = builder.build();

        Map<String, Table> tables = new HashMap<>();
        for (Map.Entry<String, List<SchemaField>> entry : mixpanelFields.entrySet()) {
            Map<String, SchemaField> fieldMap = new HashMap<>();
            tables.put(entry.getKey(), new Table(convertRakamName(entry.getKey()), fieldMap));

            for (SchemaField schemaField : entry.getValue()) {
                if (BLACKLIST.contains(schemaField.getName()))
                    continue;
                SchemaField f = new SchemaField(convertRakamName(schemaField.getName()), schemaField.getType(), schemaField.isNullable());
                while (hasAttribute(fieldMap, f.getName())) {
                    f = new SchemaField(f.getName() + "_", schemaField.getType(), schemaField.isNullable());
                }
                fieldMap.put(schemaField.getName(), f);
            }
            fieldMap.put("time", new SchemaField("_time", FieldType.LONG, false));
            fieldMap.put("$referrer", new SchemaField("_referrer", FieldType.LONG, false));
            fieldMap.put("$referring_domain", new SchemaField("_referring_domain", FieldType.LONG, false));
            fieldMap.put("mp_lib", new SchemaField("mp_lib", FieldType.LONG, false));
            fieldMap.put("distinct_id", new SchemaField("distinct_id", FieldType.STRING, false));
            fieldMap.put("$search_engine", new SchemaField("search_engine", FieldType.STRING, false));
        }

        return tables;
    }


    public static class TopType {
        public final long count;
        public final String type;

        @JsonCreator
        public TopType(@JsonProperty("count") long count,
                       @JsonProperty("type") String type) {
            this.count = count;
            this.type = type;
        }
    }

    public enum MixpanelType {
        string("string", FieldType.STRING),
        number("number", FieldType.DOUBLE),
        bool("boolean", FieldType.BOOLEAN),
        datetime("datetime", FieldType.TIMESTAMP),
        unknown("unknown", FieldType.STRING);

        private final String mixpanelType;
        private final FieldType type;

        MixpanelType(String mixpanelType, FieldType type) {
            this.mixpanelType = mixpanelType;
            this.type = type;
        }

        public static MixpanelType fromMixpanelType(String str) {
            for (MixpanelType mixpanelType : values()) {
                if(mixpanelType.mixpanelType.equals(str)) {
                    return mixpanelType;
                }
            }
            throw new IllegalArgumentException("type is now found: "+str);
        }

    }

    public void importEventsFromMixpanel(String mixpanelEventType, String rakamCollection, Map<String, SchemaField> properties, LocalDate startDate, LocalDate endDate, int projectTimezoneOffset, Consumer<List<RakamClient.Event>> consumer) {

        Map<String, String> build = ImmutableMap.<String, String>builder()
                .put("event", "[\"" + mixpanelEventType + "\"]")
                .put("from_date", startDate.format(DateTimeFormatter.ISO_LOCAL_DATE))
                .put("to_date", endDate.format(DateTimeFormatter.ISO_LOCAL_DATE)).build();

        LOGGER.info("Sending export request to Mixpanel for time period %s and %s..",
                ISO_DATE.format(startDate), ISO_DATE.format(endDate));
        InputStream export = generateRequest("export", apiKey, secretKey, build);
        Scanner scanner = new Scanner(export);
        scanner.useDelimiter("\n");
        Map<String, String> nameCache = properties == null ? new HashMap() : null;

        LOGGER.info("Mixpanel returned events performed between %s and %s. Started processing data and sending to Rakam..",
                ISO_DATE.format(startDate), ISO_DATE.format(endDate));
        RakamClient.Event[] batchRecords = new RakamClient.Event[10000];

        for (int i = 0; i < batchRecords.length; i++) {
            batchRecords[i] = new RakamClient.Event(null, rakamCollection, new HashMap<>());
        }

        int idx = 0, batch = 0;
        while (scanner.hasNext()) {
            MixpanelEvent read = JsonHelper.read(scanner.next(), MixpanelEvent.class);
            Map record = (Map) batchRecords[idx++].properties;

            for (Map.Entry<String, Object> entry : read.properties.entrySet()) {
                if (idx == batchRecords.length) {
                    LOGGER.info("Sending event batch to Rakam. Offset: %d, Current Batch: %d",
                            batch++ * batchRecords.length, batchRecords.length);
                    consumer.accept(Arrays.asList(batchRecords));
                    idx = 0;
                }

                Object value;
                if (entry.getKey().equals("time")) {
                    // adjust timezone to utc
                    value = ((Number) entry.getValue()).intValue() - projectTimezoneOffset;
                } else {
                    value = entry.getValue();
                }

                if (BLACKLIST.contains(entry.getKey()))
                    continue;

                if (properties != null) {
                    SchemaField schemaField = properties.get(entry.getKey());
                    if (schemaField == null) {
                        continue;
                    }
                    record.put(schemaField.getName(), value);
                } else {
                    String key = nameCache.get(entry.getKey());
                    if (key == null) {
                        key = convertRakamName(entry.getKey());
                        if(key.equals("time")) {
                            key = "_time";
                        }
                        nameCache.put(entry.getKey(), key);
                    }
                    record.put(key, value);
                }


            }
        }

        LOGGER.info("Sending last event batch to Rakam. Offset: %d, Current Batch: %d",
                batch*batchRecords.length, idx);
        consumer.accept(Arrays.asList(Arrays.copyOfRange(batchRecords, 0, idx)));
    }

    public void importPeopleFromMixpanel(String rakamProject, Map<String, SchemaField> properties, LocalDate lastSeen, Consumer<List<User>> consumer) {
        ImmutableMap<String, String> build = lastSeen == null ? ImmutableMap.<String, String>of() :
                ImmutableMap.of("selector", String.format("datetime(%d) >= properties[\"$last_seen\"]", lastSeen.atStartOfDay().toEpochSecond(ZoneOffset.UTC)));
        EngageResult engage = JsonHelper.read(generateRequestAndParse("engage", apiKey, secretKey, build), EngageResult.class);
        do {
            consumer.accept(engage.results.stream().map(r -> {
                Map<String, Object> userProps;
                if(properties == null) {
                    userProps = r.properties.entrySet().stream()
                            .filter(e -> e.getValue() != null)
                            .collect(Collectors.toMap(e -> convertRakamName(e.getKey()), e -> e.getValue()));
                } else {
                    userProps = r.properties.entrySet().stream()
                            .filter(e -> properties.containsKey(e.getKey()))
                            .filter(e -> e.getValue() != null)
                            .collect(Collectors.toMap(e -> properties.get(e.getKey()).getName(), e -> e.getValue()));
                }

                return new User(rakamProject, r.id, userProps);
            }).collect(Collectors.toList()));

            engage = JsonHelper.read(generateRequestAndParse("engage", apiKey, secretKey,
                    ImmutableMap.of("session_id", engage.session_id, "page", Long.toString(engage.page + 1))), EngageResult.class);
        } while (engage.results.size() >= engage.page_size);
    }
}