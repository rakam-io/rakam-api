package org.rakam.util;

import org.joda.time.format.*;

import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

public class DateTimeUtils {
    public static final java.time.format.DateTimeFormatter TIMESTAMP_FORMATTER = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);
    public static final java.time.format.DateTimeFormatter LOCAL_DATE_FORMATTER = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter DATE_FORMATTER = ISODateTimeFormat.date().withZoneUTC();
    private static final DateTimeFormatter TIMESTAMP_WITHOUT_TIME_ZONE_FORMATTER;
    private static final DateTimeFormatter TIMESTAMP_WITH_TIME_ZONE_FORMATTER;

    static {
        DateTimeParser[] timestampWithoutTimeZoneParser = {
                DateTimeFormat.forPattern("yyyy-M-d").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s.SSS").getParser()};
        DateTimePrinter timestampWithoutTimeZonePrinter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").getPrinter();
        TIMESTAMP_WITHOUT_TIME_ZONE_FORMATTER = new DateTimeFormatterBuilder()
                .append(timestampWithoutTimeZonePrinter, timestampWithoutTimeZoneParser)
                .toFormatter()
                .withOffsetParsed();

        DateTimeParser[] timestampWithTimeZoneParser = {
                DateTimeFormat.forPattern("yyyy-M-dZ").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d Z").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:mZ").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m Z").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:sZ").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s Z").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s.SSSZ").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s.SSS Z").getParser(),
                DateTimeFormat.forPattern("yyyy-M-dZZZ").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d ZZZ").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:mZZZ").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m ZZZ").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:sZZZ").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s ZZZ").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s.SSSZZZ").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s.SSS ZZZ").getParser()};
        DateTimePrinter timestampWithTimeZonePrinter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS ZZZ").getPrinter();
        TIMESTAMP_WITH_TIME_ZONE_FORMATTER = new DateTimeFormatterBuilder()
                .append(timestampWithTimeZonePrinter, timestampWithTimeZoneParser)
                .toFormatter()
                .withOffsetParsed();
    }

    private DateTimeUtils() {
    }

    public static int parseDate(String value) {
        return (int) TimeUnit.MILLISECONDS.toDays(DATE_FORMATTER.parseMillis(value));
    }

    public static long parseTimestamp(Number timestampWithTimeZone) {
        return timestampWithTimeZone.longValue();
    }

    public static long parseTimestamp(Object timestampWithTimeZone) {
        if (timestampWithTimeZone instanceof Number) {
            return parseTimestamp((Number) timestampWithTimeZone);
        } else if (timestampWithTimeZone instanceof String) {
            String encoded = timestampWithTimeZone.toString();
            // Joda parses [0-9]{10} as TIMESTAMP with huge value so we limit the characters.
            if (encoded.length() > 12) {
                parseTimestamp(encoded);
            }
        }

        throw new RuntimeException("Invalid TIMESTAMP");
    }

    public static long parseTimestamp(String timestampWithTimeZone) {
        if (timestampWithTimeZone.length() <= 12) {
            throw new IllegalArgumentException();
        }

        try {
            return ISODateTimeFormat.dateTimeParser().parseMillis(timestampWithTimeZone);
        } catch (Exception e) {
            try {
                return TIMESTAMP_WITHOUT_TIME_ZONE_FORMATTER.parseMillis(timestampWithTimeZone);
            } catch (Exception ex) {
                return TIMESTAMP_WITH_TIME_ZONE_FORMATTER.parseMillis(timestampWithTimeZone);
            }
        }
    }
}
