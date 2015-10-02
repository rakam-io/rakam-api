package org.rakam.util;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;


public class ByteBufferOutputStream extends OutputStream {
    private ByteBuffer byteBuffer;

    public ByteBufferOutputStream (ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    public ByteBuffer getByteBuffer () {
        return byteBuffer;
    }

    public void setByteBuffer (ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    public void write (int b) throws IOException {
        if (!byteBuffer.hasRemaining()) flush();
        byteBuffer.put((byte)b);
    }

    public void write (byte[] bytes, int offset, int length) throws IOException {
        if (byteBuffer.remaining() < length) flush();
        byteBuffer.put(bytes, offset, length);
    }
}
