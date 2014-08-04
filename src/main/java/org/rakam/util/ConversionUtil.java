package org.rakam.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by buremba <Burak Emre Kabakcı> on 22/07/14 01:09.
 */
public class ConversionUtil {
    public static Integer toInt(Object expectInt, Integer elseInt) throws IllegalArgumentException {
        if (expectInt!=null) {
            if (expectInt instanceof Number)
                return (Integer) expectInt;
            else
                try {
                    return Integer.parseInt((String) expectInt);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException();
                }
        }
        return elseInt;
    }

    public static Integer toInt(Object expectInt) throws IllegalArgumentException {
        Integer integer = toInt(expectInt, null);
        if(integer==null)
            throw new IllegalArgumentException();
        else
            return integer;
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
