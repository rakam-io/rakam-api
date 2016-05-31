package org.rakam.collection;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.DateTimePrinter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class DateTimeUtils {
    private DateTimeUtils()
    {
    }

    private static final DateTimeFormatter DATE_FORMATTER = ISODateTimeFormat.date().withZoneUTC();

    public static int parseDate(String value)
    {
        return (int) TimeUnit.MILLISECONDS.toDays(DATE_FORMATTER.parseMillis(value));
    }

    public static String printDate(int days)
    {
        return DATE_FORMATTER.print(TimeUnit.DAYS.toMillis(days));
    }

    private static final DateTimeFormatter TIMESTAMP_WITHOUT_TIME_ZONE_FORMATTER;
    private static final DateTimeFormatter TIMESTAMP_WITH_TIME_ZONE_FORMATTER;
    private static final DateTimeFormatter TIMESTAMP_WITH_OR_WITHOUT_TIME_ZONE_FORMATTER;

    static {
        DateTimeParser[] timestampWithoutTimeZoneParser = {
                DateTimeFormat.forPattern("yyyy-M-d").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s.SSS").getParser()};
        DateTimePrinter timestampWithoutTimeZonePrinter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").getPrinter();
        TIMESTAMP_WITHOUT_TIME_ZONE_FORMATTER = new org.joda.time.format.DateTimeFormatterBuilder()
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
        TIMESTAMP_WITH_TIME_ZONE_FORMATTER = new org.joda.time.format.DateTimeFormatterBuilder()
                .append(timestampWithTimeZonePrinter, timestampWithTimeZoneParser)
                .toFormatter()
                .withOffsetParsed();

        DateTimeParser[] timestampWithOrWithoutTimeZoneParser = Stream.concat(Stream.of(timestampWithoutTimeZoneParser), Stream.of(timestampWithTimeZoneParser))
                .toArray(DateTimeParser[]::new);
        TIMESTAMP_WITH_OR_WITHOUT_TIME_ZONE_FORMATTER = new org.joda.time.format.DateTimeFormatterBuilder()
                .append(timestampWithTimeZonePrinter, timestampWithOrWithoutTimeZoneParser)
                .toFormatter()
                .withOffsetParsed();
    }

    public static long parseTimestampLiteral(String value)
    {
        try {
            return TIMESTAMP_WITH_TIME_ZONE_FORMATTER.parseDateTime(value).getMillis();
        }
        catch (Exception e) {
            return TIMESTAMP_WITHOUT_TIME_ZONE_FORMATTER.parseMillis(value);
        }
    }

    public static long parseTimestampWithTimeZone(String timestampWithTimeZone)
    {
        return TIMESTAMP_WITH_OR_WITHOUT_TIME_ZONE_FORMATTER.parseDateTime(timestampWithTimeZone).getMillis();
    }


}
