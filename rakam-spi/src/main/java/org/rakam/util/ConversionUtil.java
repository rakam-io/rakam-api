package org.rakam.util;


public class ConversionUtil {
    // assumes an object is a number, if it can't parse it tries to parse it.
    public static Long toLong(Object expectInt) {
        if (expectInt != null) {
            try {
                return ((Number) expectInt).longValue();
            } catch (ClassCastException e) {
                if (expectInt instanceof String) {
                    try {
                        return Long.parseLong((String) expectInt);
                    } catch (NumberFormatException e1) {}
                }
                return null;
            }
        }
        return null;
    }
}
