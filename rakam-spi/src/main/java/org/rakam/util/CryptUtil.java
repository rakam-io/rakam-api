package org.rakam.util;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Random;

public class CryptUtil {
    static final Random random = new SecureRandom();

    public static String generateKey(int length) {
        String key;
        while((key=new BigInteger(length*5/*base 32,2^5*/, random).toString(32)).length()<length);
        return key;
    }
}
