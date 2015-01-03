package org.rakam.database.rakamdb;

import com.google.common.hash.PrimitiveSink;
import org.rakam.kume.Cluster;
import org.rakam.kume.Member;
import org.rakam.kume.service.ringmap.AbstractRingMap;
import org.rakam.kume.transport.serialization.SinkSerializable;
import org.rakam.kume.util.ConsistentHashRing;
import org.rakam.util.Interval;
import org.rakam.util.NotImplementedException;
import org.rakam.util.json.JsonObject;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/01/15 14:36.
 */
public class RakamDB extends AbstractRingMap<RakamDB, Map, RakamDB.IntervalColumnPair, RakamDB.ColumnStore> {
    private final Interval interval;
    ConsistentHashRing ring;

    public RakamDB(Cluster.ServiceContext<RakamDB> ctx, Interval tableInterval, int replicationFactor) {
        super(ctx, Table::new, (first, other) -> first, replicationFactor);
        ring = new ConsistentHashRing(ctx.getCluster().getMembers(), 8, 2);
        interval = tableInterval;
    }

    public void addEvent(String tableName, JsonObject json) {
        // TODO: send requests at once by aggregating data to member -> requests.
        ArrayList<CompletableFuture> listeners = new ArrayList<>();
        for (Map.Entry<String, Object> entry : json) {
            long l = ConsistentHashRing.newHasher()
                    .putString(tableName, Charset.defaultCharset())
                    .putString(entry.getKey(), Charset.defaultCharset())
                    .putInt(interval.spanCurrent().current()).hash().asLong();
            ConsistentHashRing.Bucket bucket = ring.findBucketFromToken(l);

            ArrayList<Member> members = bucket.members;
            for (Member member : members) {
                String column = entry.getKey();
                Object value = entry.getValue();
                if(value==null) continue;
                CompletableFuture<Object> task = getContext()
                        .tryAskUntilDone(member, (service, ctx) -> {
                            service.addColumnValueLocal(tableName, column, value);
                            ctx.reply(true);
                        }, 5);
                listeners.add(task);
            }
        }
        listeners.forEach(l -> l.join());
    }

    public void addColumnValueLocal(String tableName, String column, Object value) {
        IntervalColumnPair key = new IntervalColumnPair(tableName, column, interval.spanCurrent().current());
        Map<IntervalColumnPair, ColumnStore> partition = getPartition(ring.findBucketId(key));
        ColumnStore columnStore = partition
                .computeIfAbsent(key,
                        intervalColumnPair -> {
                            ValueType type = ValueType.determineType(value);
                            return ColumnStore.createColumnStore(tableName + ":" + column, type);
                        });

        columnStore.put(value);
    }

    public static class Table extends ConcurrentHashMap<IntervalColumnPair, ColumnStore> {
    }

    public static enum ValueType {
        LONG, FLOAT, INTEGER, UNSIGNED_INTEGER, STRING;

        public static ValueType determineType(Object obj) {
            if (obj instanceof Long) return LONG;
            if (obj instanceof String) return STRING;
            if (obj instanceof Integer) return INTEGER;
            if (obj instanceof Number) return FLOAT;
            throw new IllegalArgumentException("unsupported type : " + obj.getClass());
        }
    }

    public static abstract class ColumnStore {
        private final FileChannel fileChannel;
        ByteBuffer storage;
        int position = 0;

        static long bufferSize = 8 * 1000;

        public ColumnStore(String name) {
            try {
                File f = new File("/tmp/" + name);
                f.delete();

                fileChannel = new RandomAccessFile(f, "rw").getChannel();
                storage = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, bufferSize);
            } catch (IOException e) {
                throw new IllegalStateException();
            }
        }

        public void put(Object value) {
            if (!storage.hasRemaining()) {
                position += storage.position();
                try {
                    storage = fileChannel.map(FileChannel.MapMode.READ_WRITE, position, bufferSize);
                } catch (IOException e) {
                    throw new IllegalStateException();
                }
            }
            add(value);
        }

        public abstract void add(Object value);

        public static ColumnStore createColumnStore(String storeName, ValueType type) {
            switch (type) {
                case INTEGER:
                    return new IntegerColumnStore(storeName);
                case FLOAT:
                    return new FloatColumnStore(storeName);
                case LONG:
                    return new LongColumnStore(storeName);
                case STRING:
                    return new StringColumnStore(storeName);
                case UNSIGNED_INTEGER:
                    throw new NotImplementedException();
                default:
                    throw new UnsupportedOperationException("class type is not supported");
            }

        }
    }

    public static class FloatColumnStore extends ColumnStore {

        public FloatColumnStore(String name) {
            super(name);
        }

        @Override
        public void add(Object value) {
            storage.putFloat((Float) value);
        }
    }

    public static class IntegerColumnStore extends ColumnStore {

        public IntegerColumnStore(String name) {
            super(name);
        }

        @Override
        public void add(Object value) {
            storage.putInt((Integer) value);
        }
    }


    public static class LongColumnStore extends ColumnStore {

        public LongColumnStore(String name) {
            super(name);
        }

        @Override
        public void add(Object value) {
            storage.putLong((Long) value);
        }
    }

    public static class StringColumnStore extends ColumnStore {
        private final FileChannel indexFileChannel;
        MappedByteBuffer index;
        static Charset charset = Charset.forName("UTF-8");

        public StringColumnStore(String name) {
            super(name);
            try {
                File f = new File("/tmp/" + name + ".index");
                f.delete();

                indexFileChannel = new RandomAccessFile(f, "rw").getChannel();
                index = indexFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, bufferSize);
            } catch (IOException e) {
                throw new IllegalStateException();
            }
        }


        @Override
        public void add(Object value) {
            byte[] bytes = ((String) value).getBytes(charset);
            index.putInt(storage.position());
            storage.putShort((short) bytes.length);
            storage.put(bytes);
        }
    }

    public static class IntervalColumnPair implements SinkSerializable {
        public final int startTimestamp;
        public final String columnName;
        public final String tableName;

        public IntervalColumnPair(String tableName, String columnName, int startTimestamp) {
            this.startTimestamp = startTimestamp;
            this.columnName = columnName;
            this.tableName = tableName;
        }

        @Override
        public void writeTo(PrimitiveSink sink) {
            sink.putInt(startTimestamp);
            sink.putString(tableName, Charset.forName("UTF-8"));
            sink.putString(columnName, Charset.forName("UTF-8"));
        }
    }
}
