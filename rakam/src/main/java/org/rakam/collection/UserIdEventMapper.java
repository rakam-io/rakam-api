package org.rakam.collection;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.DefaultCookie;
import org.apache.avro.generic.GenericRecord;
import org.rakam.analysis.ConfigManager;
import org.rakam.analysis.InternalConfig;
import org.rakam.plugin.EventMapper;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.List;
import java.util.UUID;

public class UserIdEventMapper implements EventMapper {
    private final LoadingCache<String, FieldType> userType;
    DistributedIdGenerator idGenerator;

    public UserIdEventMapper(ConfigManager configManager) {
        idGenerator = new DistributedIdGenerator();
        userType = CacheBuilder.newBuilder().build(new CacheLoader<String, FieldType>() {
            @Override
            public FieldType load(String key) throws Exception {
                return configManager.setConfigOnce(key, InternalConfig.USER_TYPE.name(), FieldType.STRING);
            }
        });
    }

    @Override
    public List<Cookie> map(Event event, HttpHeaders extraProperties, InetAddress sourceAddress, HttpHeaders responseHeaders) {
        GenericRecord properties = event.properties();

        if(properties.get("_user") == null && "true".equals(extraProperties.get("_track_user"))) {
            switch (userType.getUnchecked(event.project())) {
                case STRING:
                    String randomUUID = UUID.randomUUID().toString();
                    properties.put("_user", randomUUID);
                    return ImmutableList.of(new DefaultCookie("_user", randomUUID));
                case INTEGER:
                    long userId = idGenerator.generateId();
                    properties.put("_user", userId);
                    return ImmutableList.of(new DefaultCookie("_user", Long.toString(userId)));
                case LONG:
                    int id = (int) idGenerator.generateId();
                    properties.put("_user", id);
                    return ImmutableList.of(new DefaultCookie("_user", Integer.toString(id)));
            }
        }

        return null;
    }

    /**
     * This class implements an ID generator.
     *
     * The ID is a signed 64 bit long composed of:
     *
     * sign      - 1 bit
     * timestamp - 41 bits (millisecond precision with a custom epoch allowing for 69 years)
     * host id   - 10 bits (allowing for 1024 hosts)
     * sequence  - 12 bits (allowing for 4096 IDs per millisecond)
     *
     * There is a check that catches sequence rollover within the current millisecond.
     *
     * @author Maxim Khodanovich
     */
    public static class DistributedIdGenerator {
        private static final long START_EPOCH = 1464307172048L;

        private static final long SEQUENCE_BITS = 12L;
        private static final long SEQUENCE_MASK = -1L ^ (-1L << SEQUENCE_BITS);

        private static final long HOST_ID_BITS = 10L;
        private static final long HOST_ID_MAX = -1L ^ (-1L << HOST_ID_BITS);
        private static final long HOST_ID_SHIFT = SEQUENCE_BITS;

        private static final long TIMESTAMP_SHIFT = SEQUENCE_BITS + HOST_ID_BITS;

        private final long hostId;
        private volatile long lastTimestamp = -1L;
        private volatile long sequence = 0L;

        public DistributedIdGenerator() {
            hostId = getHostId();
            if (hostId < 0 || hostId > HOST_ID_MAX) {
                throw new IllegalStateException("Invalid host ID: " + hostId);
            }
        }

        public long generateId() throws IllegalStateException {
            long timestamp = System.currentTimeMillis();
            if (lastTimestamp == timestamp) {
                sequence = (sequence + 1) & SEQUENCE_MASK;
            } else {
                sequence = 0;
            }
            lastTimestamp = timestamp;
            return ((timestamp - START_EPOCH) << TIMESTAMP_SHIFT) | (hostId << HOST_ID_SHIFT) | sequence;
        }

        private long nextTimestamp(long lastTimestamp) {
            long timestamp = System.currentTimeMillis();
            while (timestamp <= lastTimestamp) {
                timestamp = System.currentTimeMillis();
            }
            return timestamp;
        }

        private long getHostId() throws IllegalStateException {
            try {
                NetworkInterface iface = NetworkInterface.getByInetAddress(getHostAddress());
                byte[] mac = iface.getHardwareAddress();
                return ((0x000000FF & (long) mac[mac.length - 1]) | (0x0000FF00 & (((long) mac[mac.length - 2]) << 8))) >> 6;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to get host ID", e);
            }
        }

        private InetAddress getHostAddress() throws IOException {
            InetAddress address = null;

            // Iterate all the network interfaces
            for (Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces(); ifaces.hasMoreElements();) {
                NetworkInterface iface = ifaces.nextElement();
                // Iterate all the addresses assigned to the network interface
                for (Enumeration<InetAddress> addrs = iface.getInetAddresses(); addrs.hasMoreElements();) {
                    InetAddress addr = addrs.nextElement();
                    if (!addr.isLoopbackAddress()) {
                        if (addr.isSiteLocalAddress()) {
                            // Found a non-loopback site-local address
                            return addr;
                        }
                        if (address == null) {
                            // Found the first non-loopback, non-site-local address
                            address = addr;
                        }
                    }
                }
            }

            if (address != null) {
                // Return the first non-loopback, non-site-local address
                return address;
            }

            // Return the local host address (may be the loopback address)
            return InetAddress.getLocalHost();
        }
    }
}
