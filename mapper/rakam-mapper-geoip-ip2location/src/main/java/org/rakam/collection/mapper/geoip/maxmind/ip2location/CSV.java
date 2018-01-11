package org.rakam.collection.mapper.geoip.maxmind.ip2location;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CSV {
    private static final java.lang.String CSV_PATTERN = "\"([0-9]+)\",\"([0-9]+)\",\"([^\"]+)\",\"([^\"]+)\",\"([^\"]+)\",\"([^\"]+)\",\"([0-9.-]+)\",\"([0-9.-]+)\"";

    public final long ipStart;
    public final long ipEnd;
    public final String country;
    public final String stateProv;
    public final String city;
    public final double latitude;
    public final double longitude;

    private CSV(long ipStart, long ipEnd,
                String country, String stateProv, String city,
                double latitude, double longitude) {
        this.ipStart = ipStart;
        this.ipEnd = ipEnd;
        this.country = country;
        this.stateProv = stateProv;
        this.city = city;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public static CSV parse(String csv) {
        Pattern r = Pattern.compile(CSV_PATTERN);
        Matcher m = r.matcher(csv);

        if (m.find()) {
            return new CSV(
                    Long.parseLong(m.group(1)),
                    Long.parseLong(m.group(2)),
                    m.group(4),
                    m.group(5),
                    m.group(6),
                    Double.parseDouble(m.group(7)),
                    Double.parseDouble(m.group(8))
            );
        }

        throw new IllegalArgumentException(String.format("CSV doesn't match the pattern. (%s)", csv));
    }
}