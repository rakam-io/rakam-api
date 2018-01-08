package org.rakam.collection.mapper.geoip.maxmind.ip2location;

import org.rakam.collection.mapper.geoip.maxmind.ip2location.utils.IP4Converter;

import java.io.*;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class IPReader {
    private final NavigableMap<Long, CSV> ipLookup;

    private IPReader(NavigableMap<Long, CSV> ipLookup) {
        this.ipLookup = ipLookup;
    }

    public static IPReader build(String dbPath)
            throws IOException {
        File ipdb = new File(dbPath);
        InputStream inputStream = new FileInputStream(ipdb);
        return build(inputStream);
    }

    public static IPReader build(InputStream inputStream)
            throws IOException {
        NavigableMap<Long, CSV> lookup = new ConcurrentSkipListMap<>();

        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        while (reader.ready()) {
            CSV csv = CSV.parse(reader.readLine());
            lookup.put(csv.ipStart, csv);
        }
        reader.close();

        return new IPReader(lookup);
    }

    public GeoLocation lookup(String ipAddress)
            throws UnknownHostException {
        InetAddress inetAddress = InetAddress.getByName(ipAddress);
        return lookup(inetAddress);
    }

    public GeoLocation lookup(InetAddress inetAddress) {
        return lookup(IP4Converter.toLong(inetAddress.getAddress()));
    }

    private GeoLocation lookup(Long address) {
        Map.Entry<Long, CSV> entry = ipLookup.lowerEntry(address);
        if (entry == null) {
            return null;
        }

        CSV csv = entry.getValue();
        if (csv.ipEnd < address) {
            return null;
        }

        return GeoLocation.of(csv.country, csv.stateProv, csv.city, Coordination.of(csv.latitude, csv.longitude));
    }

    private Long toLong(byte[] address) {
        return new BigInteger(address).longValue();
    }
}
