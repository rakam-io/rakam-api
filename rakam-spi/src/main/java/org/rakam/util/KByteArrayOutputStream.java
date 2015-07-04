package org.rakam.util;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/02/15 18:21.
 */
public class KByteArrayOutputStream extends OutputStream {
    protected byte buf[];

    protected int position;

    public KByteArrayOutputStream(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("Negative initial size: "
                    + size);
        }
        buf = new byte[size];
    }

    private void ensureCapacity(int minCapacity) {
        if (minCapacity - buf.length > 0)
            grow(minCapacity);
    }

    private void grow(int minCapacity) {
        // overflow-conscious code
        int oldCapacity = buf.length;
        int newCapacity = oldCapacity << 1;
        if (newCapacity - minCapacity < 0)
            newCapacity = minCapacity;
        if (newCapacity < 0) {
            if (minCapacity < 0) // overflow
                throw new OutOfMemoryError();
            newCapacity = Integer.MAX_VALUE;
        }
        buf = Arrays.copyOf(buf, newCapacity);
    }

    public void write(int b) {
        ensureCapacity(position + 1);
        buf[position] = (byte) b;
        position += 1;
    }

    public void write(byte b[], int off, int len) {
        if ((off < 0) || (off > b.length) || (len < 0) ||
                ((off + len) - b.length > 0)) {
            throw new IndexOutOfBoundsException();
        }
        ensureCapacity(position + len);
        System.arraycopy(b, off, buf, position, len);
        position += len;
    }

    public byte[] copy(int start, int end) {
        int length = end - start;
        byte[] copy = new byte[length];
        System.arraycopy(buf, start, copy, 0, length);
        return copy;
    }

    public int position() {
        return position;
    }

    public void position(int position) {
        this.position = position;
    }

    public int remaining() {
        return buf.length - position;
    }

    public ByteBuffer getBuffer(int startPosition, int endPosition) {
        return ByteBuffer.wrap(buf, startPosition, endPosition);

    }
}
