package org.rakam.util;

import autovalue.shaded.com.google.common.common.base.Throwables;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Random;

public class CryptUtil {
    static final Random random = new SecureRandom();

    public static String generateRandomKey(int length) {
        String key;
        while ((key = new BigInteger(length * 5/*base 32,2^5*/, random).toString(32)).length() < length) ;
        return key;
    }

    public static String encryptWithHMacSHA1(String data, String secret) {
        try {
            SecretKeySpec signingKey = new SecretKeySpec(secret.getBytes("UTF-8"), "HmacSHA1");

            Mac mac = Mac.getInstance("HmacSHA1");
            mac.init(signingKey);

            byte[] rawHmac = mac.doFinal(data.getBytes("UTF-8"));

            return DatatypeConverter.printBase64Binary(rawHmac);
        } catch (NoSuchAlgorithmException | InvalidKeyException | UnsupportedEncodingException e) {
            throw Throwables.propagate(e);
        }
    }
}
