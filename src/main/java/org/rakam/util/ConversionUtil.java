package org.rakam.util;

/**
 * Created by buremba <Burak Emre Kabakcı> on 22/07/14 01:09.
 */
public class ConversionUtil {
    public static Long toLong(Object expectInt, Long elseInt) throws IllegalArgumentException {
        if (expectInt != null) {
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

    public static Long toLong(Object expectInt) {
        return toLong(expectInt, null);
    }
}
