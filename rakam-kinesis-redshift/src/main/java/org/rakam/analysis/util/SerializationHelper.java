package org.rakam.analysis.util;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Created by buremba <Burak Emre Kabakcı> on 16/07/15 17:23.
 */
public class SerializationHelper {

    public static int readInt(ByteBuffer buffer) throws IOException {
        int b = buffer.get() & 0xff;
        int n = b & 0x7f;
        if (b > 0x7f) {
            b = buffer.get() & 0xff;
            n ^= (b & 0x7f) << 7;
            if (b > 0x7f) {
                b = buffer.get() & 0xff;
                n ^= (b & 0x7f) << 14;
                if (b > 0x7f) {
                    b = buffer.get() & 0xff;
                    n ^= (b & 0x7f) << 21;
                    if (b > 0x7f) {
                        b = buffer.get() & 0xff;
                        n ^= (b & 0x7f) << 28;
                        if (b > 0x7f) {
                            throw new IOException("Invalid int encoding");
                        }
                    }
                }
            }
        }
        return (n >>> 1) ^ -(n & 1); // back to two's-complement
    }

    public static void encodeInt(int n, OutputStream out) throws IOException {
        // move sign to low-order bit, and flip others if negative
        n = (n << 1) ^ (n >> 31);
        if ((n & ~0x7F) != 0) {
            out.write((n | 0x80) & 0xFF);
            n >>>= 7;
            if (n > 0x7F) {
                out.write((n | 0x80) & 0xFF);
                n >>>= 7;
                if (n > 0x7F) {
                    out.write((n | 0x80) & 0xFF);
                    n >>>= 7;
                    if (n > 0x7F) {
                        out.write((n | 0x80) & 0xFF);
                        n >>>= 7;
                    }
                }
            }
        }
        out.write(n);
    }
}
