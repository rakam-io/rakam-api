package org.rakam.util;

import com.google.common.base.Throwables;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.AlgorithmParameters;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Random;

public final class CryptUtil {
    private static final Random random = new SecureRandom();

    private CryptUtil() {}

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

    public static String encryptAES(String data, String secretKey) {
        try {
            byte[] apiKeyBytes = secretKey.getBytes("UTF-8");

            if(apiKeyBytes.length % 16 != 0) {
                // Cipher.getMaxAllowedKeyLength("AES") / Byte.SIZE equals 16
                // we don't want to force users to install “Unlimited Strength” JCE policy files manually.
                apiKeyBytes = Arrays.copyOf(apiKeyBytes, 16);
            }

            final SecretKey secret = new SecretKeySpec(apiKeyBytes, "AES");

            final Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            cipher.init(Cipher.ENCRYPT_MODE, secret);

            final AlgorithmParameters params = cipher.getParameters();

            final byte[] iv = params.getParameterSpec(IvParameterSpec.class).getIV();
            final byte[] cipherText = cipher.doFinal(data.getBytes("UTF-8"));

            return DatatypeConverter.printHexBinary(iv) + DatatypeConverter.printHexBinary(cipherText);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }


    public static String decryptAES(String data, String secretKey) {
        try {
            byte[] apiKeyBytes = secretKey.getBytes("UTF-8");

            if(apiKeyBytes.length % 16 != 0) {
                apiKeyBytes = Arrays.copyOf(apiKeyBytes, 16);
            }

            // grab first 16 bytes (aka 32 characters of hex) - that's the IV
            String hexedIv = data.substring(0, 32);

            // grab everything else - that's the cipher-text (aka encrypted message)
            String hexedCipherText = data.substring(32);

            byte[] iv = DatatypeConverter.parseHexBinary(hexedIv);
            byte[] cipherText = DatatypeConverter.parseHexBinary(hexedCipherText);

            final SecretKey secret = new SecretKeySpec(apiKeyBytes, "AES");

            final Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");

            IvParameterSpec ivParameterSpec = new IvParameterSpec(iv);
            cipher.init(Cipher.DECRYPT_MODE, secret, ivParameterSpec);

            return new String(cipher.doFinal(cipherText), "UTF-8");
        } catch (BadPaddingException e) {
            throw new IllegalArgumentException("secret key is invalid");
        }catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
