package org.rakam.importer.mixpanel;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.log.Logger;
import org.rakam.collection.Event;
import org.rakam.importer.RakamClient;
import org.rakam.util.JsonHelper;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.io.ByteStreams.toByteArray;

@Command(name = "import", description = "Mixpanel importer")
public class MixpanelEventImporter implements Runnable {
    private final static Logger LOGGER = Logger.get(MixpanelEventImporter.class);

    @Arguments(description = "Patterns of files to be added")
    public List<String> collections;

    @Option(name="--mixpanel.api-key", description = "Api key", required = true)
    public String apiKey;

    @Option(name="--mixpanel.api-secret", description = "Api secret", required = true)
    public String apiSecret;

    @Option(name="--rakam.project", description = "Project", required = true)
    public String project;

    @Option(name="--rakam.project.write-key", description = "Project", required = true)
    public String rakamWriteKey;

    @Option(name="--rakam.address", description = "Rakam cluster url", required = true)
    public String rakamAddress;

    @Option(name="--start", description = "Mixpanel event start date")
    public String startDate;

    @Option(name="--end", description = "Mixpanel event end date")
    public String endDate;

    @Option(name="--duration", description = "Mixpanel event import duration")
    public String duration;

    @Option(name="--schema.file", description = "Mixpanel event schema file")
    public File schemaFile;

    @Option(name="--schema", description = "Mixpanel event schema file")
    public String schema;

    @Option(name="--mixpanel.project.timezone", description = "Mixpanel project utc.")
    public Integer projectTimezone;

    @Override
    public void run() {
        MixpanelImporter mixpanelImporter = new MixpanelImporter(apiKey, apiSecret);

        if(schemaFile != null) {
            if(schema != null) {
                throw new IllegalArgumentException("Only one of schema and schemaFile can be set");
            }

            try {
                schema = new String(Files.readAllBytes(Paths.get(schemaFile.getPath())), "UTF-8");
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        Map<String, Table> fields;
        if(schema != null) {
            fields = JsonHelper.read(schema, new TypeReference<Map<String, Table>>() {});
        } else {
            fields = null;
        }

        RakamClient client = new RakamClient(rakamAddress);

        LocalDate start = null, end = null;
        if(startDate != null) {
            start = LocalDate.parse(startDate);
        }
        if(endDate != null) {
            end = LocalDate.parse(endDate);
        }
        if(duration != null) {
            io.airlift.units.Duration duration = io.airlift.units.Duration.valueOf(this.duration);
            long days = (long) duration.convertTo(TimeUnit.DAYS).getValue();

            if(days == 0) {
                throw new IllegalArgumentException("interval is invalid.");
            }

            if(endDate == null && startDate != null) {
                end = start.plusDays(days);
            } else if(endDate != null && startDate != null) {
                throw new IllegalArgumentException("duration must not set when startDate and endDate is set");
            } else if(endDate != null && startDate == null) {
                start = end.minusDays(days);
            } else {
                // if current day is included, we may not find the offset if unique event id is not imported.
                end = LocalDate.now();
                start = end.minusDays(days);
            }
        }

        if(end == null) {
            end = LocalDate.now();
        }

        if(start == null) {
            start = LocalDate.of(2011, 7, 10);
        }

        if(projectTimezone == null) {
            projectTimezone = 0;
        } else {
            projectTimezone *= 60 * 60;
        }

        final LocalDate finalStart = start;
        final LocalDate finalEnd = end;
        if(fields != null) {
            fields.entrySet().parallelStream().filter(c -> collections == null || !collections.contains(c.getKey())).forEach(entry -> {
                try {
                    mixpanelImporter.importEventsFromMixpanel(entry.getKey(), entry.getValue().rakamCollection, entry.getValue().mapping, finalStart, finalEnd, projectTimezone,
                            (events) -> {
                                Event.EventContext context = new Event.EventContext(rakamWriteKey, null, null, null);
                                client.batchEvents(new RakamClient.EventList(context, project, events));
                            });
                } catch (Exception e) {
                    LOGGER.error(e, "Unable to import collection "+entry.getKey());
                }
            });
        } else {
            mixpanelImporter.getCollections().stream().filter(c -> collections == null || !collections.contains(c)).forEach(collection -> {
                try {
                    mixpanelImporter.importEventsFromMixpanel(collection, convertRakamName(collection), null, finalStart, finalEnd, projectTimezone,
                            (events) -> {
                                Event.EventContext context = new Event.EventContext(rakamWriteKey, null, null, null);
                                client.batchEvents(new RakamClient.EventList(context, project, events));
                            });
                } catch (Exception e) {
                    LOGGER.error(e, "Unable to import collection "+collection);
                }
            });
        }
    }

    public static byte[] generateRequestAndParse(String path, String apiKey, String secretKey, Map<String, String> build) {
        try {
            return toByteArray(generateRequest(path, apiKey, secretKey, build));
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static InputStream generateRequest(String path, String apiKey, String secretKey, Map<String, String> build) {
        build = ImmutableMap.<String, String>builder().putAll(build)
                .put("expire", Long.toString(Instant.now().plus(2, ChronoUnit.HOURS).getEpochSecond()))
                .put("api_key", apiKey)
                .build();

        String collect = build.entrySet().stream()
                .sorted((o1, o2) -> o1.getKey().compareTo(o2.getKey()))
                .map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining("")) + secretKey;

        String signature;
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            signature = DatatypeConverter.printHexBinary(md.digest(collect.getBytes(Charset.forName("UTF-8"))));
        } catch (NoSuchAlgorithmException e) {
            throw Throwables.propagate(e);
        }

        String encodedUrlString = build.entrySet().stream().map(e -> {
            try {
                return e.getKey() + "=" + URLEncoder.encode(e.getValue(), "UTF-8");
            } catch (UnsupportedEncodingException e1) {
                return e.getValue();
            }
        }).collect(Collectors.joining("&")) + "&sig=" + signature;
        try {
            return new URL(String.format("https://%smixpanel.com/api/2.0/%s/?", path.equals("export") ? "data." : "", path) + encodedUrlString).openStream();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static String convertRakamName(String name) {
        return name.replaceAll("[^a-zA-Z0-9_]+", "_")
                .replaceAll("(.)(\\p{Lu})", "$1_$2")
                .replaceAll("__", "_")
                .toLowerCase(Locale.ENGLISH);
    }
}
