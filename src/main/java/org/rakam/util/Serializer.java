package org.rakam.util;

import org.apache.cassandra.utils.ByteBufferUtil;

import java.nio.ByteBuffer;
import java.util.Collection;

/**
 * Created by buremba on 24/05/14.
 */
public class Serializer {
    public static ByteBuffer serialize(Collection<String> list) {

        int size = 0;
        for (String elt : list) {
            size += 2 + ByteBufferUtil.bytes(elt).remaining();
        }

        ByteBuffer result = ByteBuffer.allocate(2 + size);
        result.putShort((short) list.size());
        for (String bb : list) {
            ByteBuffer b = ByteBufferUtil.bytes(bb);
            result.putShort((short) b.remaining());
            result.put(b.duplicate());
        }
        return (ByteBuffer) result.flip();
    }
}
