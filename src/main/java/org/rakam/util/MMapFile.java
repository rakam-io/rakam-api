package org.rakam.util;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UTFDataFormatException;
import java.nio.BufferUnderflowException;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by buremba <Burak Emre Kabakcı> on 04/01/15 22:01.
 */
/**
 * Some of the methods are taken from NativeBytes.java of OpenFTH Java-Util library
 * @author peter.lawrey
 */
public class MMapFile {
    private final FileChannel fileChannel;
    private final String path;
    private final FileChannel.MapMode mode;
    private MappedByteBuffer storage;
    private static final int UNSIGNED_SHORT_MASK = 0xFFFF;
    private static final int UNSIGNED_BYTE_MASK = 0xFF;
    private StringBuilder utfReader = null;
    private StringInterner stringInterner = null;

    private final long block_size;
    private long offset;

    public MMapFile(String path, FileChannel.MapMode mode, int block_size) throws IOException {
        File f = new File(path);
        RandomAccessFile rw = new RandomAccessFile(f, mode.equals(FileChannel.MapMode.READ_WRITE) ? "rw" : "r");
//        rw.setLength(block_size);
        fileChannel = rw.getChannel();
        storage = fileChannel.map(mode, offset, block_size);
        storage.order(ByteOrder.nativeOrder());
        this.block_size = block_size;
        this.path = path;
        this.mode = mode;
    }

    public void ensureCapacity(int length) throws IOException {
        if (storage.remaining() <= length) {
            System.out.print(hashCode()+" changing position of " + path+" new offset "+offset);
            offset += storage.position();
            System.out.println(" to " + offset);
            storage.force();
            storage = fileChannel.map(mode, offset, block_size);
        }
    }

    public int position() {
        return storage.position();
    }

    public void force() {
         storage.force();
    }

    public void load() {
         storage.load();
    }

    public long memoryAddress() {
        return offset;
    }

    public void writeInt(int value) {
        storage.putInt(value);
    }

    public int readInt() {
        return storage.getInt();
    }

    public void writeFloat(float value) {
        storage.putFloat(value);
    }

    public void writeLong(long value) {
        storage.putLong(value);
    }

    public void writeUTF(String str) {
        int strlen = str.length();
        long utflen = 0; /* use charAt instead of copying String to char array */
        byte c;
        for (int i = 0; i < strlen; i++) {
            c = (byte) str.charAt(i);
            if ((c >= 0x0000) && (c <= 0x007F)) {
                utflen++;
            } else if (c > 0x07FF) {
                utflen += 3;
            } else {
                utflen += 2;
            }
        }
        if (utflen > 65535)
            throw new IllegalStateException("String too long " + utflen + " when encoded, max: 65535");
        writeUnsignedShort((int) utflen);

        int i;
        for (i = 0; i < strlen; i++) {
            c = (byte) str.charAt(i);
            if (!((c >= 0x0000) && (c <= 0x007F)))
                break;
            storage.put(c);
        }

        for (; i < strlen; i++) {
            c = (byte) str.charAt(i);
            if ((c >= 0x0000) && (c <= 0x007F)) {
                storage.put(c);

            } else if (c > 0x07FF) {
                storage.put((byte) (0xE0 | ((c >> 12) & 0x0F)));
                storage.put((byte) (0x80 | ((c >> 6) & 0x3F)));
                storage.put((byte) (0x80 | (c & 0x3F)));
            } else {
                storage.put((byte) (0xC0 | ((c >> 6) & 0x1F)));
                storage.put((byte) (0x80 | c & 0x3F));
            }
        }
    }

    private void writeUnsignedShort(int v) {
        storage.putShort((short) v);
    }

    public int readUnsignedShort() {
        return storage.getShort() & UNSIGNED_SHORT_MASK;
    }

    private StringBuilder acquireUtfReader() {
        if (utfReader == null)
            utfReader = new StringBuilder(128);
        else
            utfReader.setLength(0);
        return utfReader;
    }

    private StringInterner stringInterner() {
        if (stringInterner == null)
            stringInterner = new StringInterner(8 * 1024);
        return stringInterner;
    }

    public String readUTF() {
        try {
            int len = readUnsignedShort();
            readUTF0(acquireUtfReader(), len);
            return utfReader.length() == 0 ? "" : stringInterner().intern(utfReader);
        } catch (IOException unexpected) {
            throw new AssertionError(unexpected);
        }
    }

    private void readUTF0(Appendable appendable, int utflen) throws IOException {
        int count = 0;
        while (count < utflen) {
            if(remaining() < 1)
                throw new BufferUnderflowException();
            int c = readUnsignedByte();
            if (c >= 128) {
                position(position() - 1);
                break;
            } else if (c < 0) {

            }
            count++;
            appendable.append((char) c);
        }

        while (count < utflen) {
            int c = readUnsignedByte();
            switch (c >> 4) {
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                /* 0xxxxxxx */
                    count++;
                    appendable.append((char) c);
                    break;
                case 12:
                case 13: {
                /* 110x xxxx 10xx xxxx */
                    count += 2;
                    if (count > utflen)
                        throw new UTFDataFormatException("malformed input: partial character at end");
                    int char2 = readUnsignedByte();
                    if ((char2 & 0xC0) != 0x80)
                        throw new UTFDataFormatException("malformed input around byte " + count);
                    int c2 = (char) (((c & 0x1F) << 6) |
                            (char2 & 0x3F));
                    appendable.append((char) c2);
                    break;
                }
                case 14: {
                /* 1110 xxxx 10xx xxxx 10xx xxxx */
                    count += 3;
                    if (count > utflen)
                        throw new UTFDataFormatException(
                                "malformed input: partial character at end");
                    int char2 = readUnsignedByte();
                    int char3 = readUnsignedByte();

                    if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
                        throw new UTFDataFormatException(
                                "malformed input around byte " + (count - 1));
                    int c3 = (char) (((c & 0x0F) << 12) |
                            ((char2 & 0x3F) << 6) |
                            (char3 & 0x3F));
                    appendable.append((char) c3);
                    break;
                }
                default:
                /* 10xx xxxx, 1111 xxxx */
                    throw new UTFDataFormatException("malformed input around byte " + count);
            }
        }
    }

    private int remaining() {
        return storage.remaining();
    }


    public int readUnsignedByte() {
        return storage.get() & UNSIGNED_BYTE_MASK;
    }

    private void position(int p0) {
        storage.position(p0);
    }

}
