package org.rakam.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by buremba <Burak Emre Kabakcı> on 22/07/14 01:09.
 */
public class ConversionUtil {
    public static Long toLong(Object expectInt, Long elseInt) throws IllegalArgumentException {
        if (expectInt!=null) {
            if (expectInt instanceof Number)
                return ((Number) expectInt).longValue();
            else
                try {
                    return Long.parseLong((String) expectInt);
                } catch (NumberFormatException e) {
                    return null;
                }
        }
        return elseInt;
    }

    public static Long toLong(Object expectInt) throws IllegalArgumentException {
        return toLong(expectInt, null);
    }

    public static Integer parseDate(String str) {
        try {
            return Integer.parseInt(str);
        } catch (Exception e) {
            try {
                return (int) (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(str).getTime() / 1000);
            } catch (ParseException ex) {
                return null;
            }
        }
    }
}
