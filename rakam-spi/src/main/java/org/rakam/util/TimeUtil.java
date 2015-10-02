package org.rakam.util;

import java.time.Clock;
import java.time.ZoneId;


public class TimeUtil {
    final static Clock clock = Clock.tickSeconds(ZoneId.of("UTC"));

    public static final int MINUTE = 60;
    public static final int SECOND = 1;
    public static final int HOUR = MINUTE;
    public static final int DAY = 86400;

    public static int UTCTime() {
        return (int) (clock.millis() / 1000);
    }
}
