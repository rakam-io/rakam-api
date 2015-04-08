package org.rakam.util;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/09/14 01:04.
 */
public class ValidationUtil {
    public static boolean equalOrNull(Object arg0, Object arg1) {
        if (arg0 == null || arg1 == null) {
            return arg0 == arg1;
        }
        return arg0.equals(arg1);
    }
}
