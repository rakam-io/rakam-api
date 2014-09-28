package org.rakam.util;

import java.util.Calendar;

/**
 * Created by buremba <Burak Emre Kabakcı> on 23/09/14 01:52.
 */
public class DateUtil {
    final static int offset = Calendar.getInstance().getTimeZone().getRawOffset();

    public static int UTCTime() {
        return (int) ((System.currentTimeMillis() + offset) / 1000);
    }

    public static long UTCTimeMillis() {
        return System.currentTimeMillis() + offset;
    }
}
