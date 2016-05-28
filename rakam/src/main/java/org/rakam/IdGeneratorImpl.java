package org.rakam;


import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

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
public class IdGeneratorImpl  {
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

    public IdGeneratorImpl() {
        hostId = getHostId();
        if (hostId < 0 || hostId > HOST_ID_MAX) {
            throw new IllegalStateException("Invalid host ID: " + hostId);
        }
    }

    public static void main(String[] args) {
        System.out.println(new IdGeneratorImpl().generateId());
    }

    public synchronized long generateId() throws IllegalStateException {
        long timestamp = System.currentTimeMillis();
        if (timestamp < lastTimestamp){
            throw new IllegalStateException("Negative timestamp delta " + (lastTimestamp - timestamp) + "ms");
        }
        if (lastTimestamp == timestamp) {
            sequence = (sequence + 1) & SEQUENCE_MASK;
            if (sequence == 0) {
                timestamp = nextTimestamp(lastTimestamp);
            }
        } else {
            sequence = 0;
        }
        lastTimestamp = timestamp;
        return ((timestamp - START_EPOCH) << TIMESTAMP_SHIFT) | (hostId << HOST_ID_SHIFT) | sequence;
    }

    private static long nextTimestamp(long lastTimestamp) {
        long timestamp = System.currentTimeMillis();
        while (timestamp <= lastTimestamp) {
            timestamp = System.currentTimeMillis();
        }
        return timestamp;
    }

    private static long getHostId() throws IllegalStateException {
        try {
            NetworkInterface iface = NetworkInterface.getByInetAddress(getHostAddress());
            byte[] mac = iface.getHardwareAddress();
            return ((0x000000FF & (long) mac[mac.length - 1]) | (0x0000FF00 & (((long) mac[mac.length - 2]) << 8))) >> 6;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to get host ID", e);
        }
    }

    private static InetAddress getHostAddress() throws IOException {
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