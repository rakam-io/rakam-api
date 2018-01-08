package org.rakam.collection.mapper.geoip.maxmind.ip2location.utils;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class IP4Converter {
    public static String toIP(Long longValue) {
        return toIP(BigInteger.valueOf(longValue).toByteArray());
    }

    public static String toIP(byte[] address) {
        int startIdx = (address.length > 4 ? 1 : 0);
        return String.format("%d.%d.%d.%d", address[startIdx] & 0xFF, address[startIdx + 1] & 0xFF, address[startIdx + 2] & 0xFF, address[startIdx + 3] & 0xFF);
    }

    public static long toLong(String ip)
            throws UnknownHostException {
        return toLong(InetAddress.getByName(ip).getAddress());
    }

    public static long toLong(byte[] address) {
        long longValue = 0;
        int startIdx = (address.length > 4 ? 1 : 0);
        for (int i = startIdx; i < address.length; i++) {
            longValue <<= 8;
            longValue += (long) (address[i] & 0xFF);
        }

        return longValue;
    }
}
