package org.rakam.util;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.DateTimePrinter;
import org.joda.time.format.ISODateTimeFormat;

import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

public class DateTimeUtils {
    private DateTimeUtils() {
    }

    public static final java.time.format.DateTimeFormatter TIMESTAMP_FORMATTER = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    private static final DateTimeFormatter DATE_FORMATTER = ISODateTimeFormat.date().withZoneUTC();
    private static final DateTimeFormatter TIMESTAMP_WITHOUT_TIME_ZONE_FORMATTER;
    private static final DateTimeFormatter TIMESTAMP_WITH_TIME_ZONE_FORMATTER;

    public static int parseDate(String value) {
        return (int) TimeUnit.MILLISECONDS.toDays(DATE_FORMATTER.parseMillis(value));
    }

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

    public static long parseTimestamp(Number timestampWithTimeZone) {
        return timestampWithTimeZone.longValue();
    }

    public static long parseTimestamp(Object timestampWithTimeZone) {
        return timestampWithTimeZone instanceof Number ?
                parseTimestamp((Number) timestampWithTimeZone) :
                parseTimestamp(timestampWithTimeZone.toString());
    }

    public static long parseTimestamp(String timestampWithTimeZone) {
        // If it's in ISO format the last character must be 'Z'
        try {
            return ISODateTimeFormat.dateTimeParser().parseMillis(timestampWithTimeZone);
        }
        catch (Exception e) {
            try {
                return TIMESTAMP_WITHOUT_TIME_ZONE_FORMATTER.parseMillis(timestampWithTimeZone);
            }
            catch (Exception ex) {
                return TIMESTAMP_WITH_TIME_ZONE_FORMATTER.parseMillis(timestampWithTimeZone);
            }
        }
    }
}
