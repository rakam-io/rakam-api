package org.rakam.database.rakamdb;

import com.google.common.hash.PrimitiveSink;
import org.rakam.kume.Cluster;
import org.rakam.kume.Member;
import org.rakam.kume.service.ringmap.AbstractRingMap;
import org.rakam.kume.transport.serialization.SinkSerializable;
import org.rakam.kume.util.ConsistentHashRing;
import org.rakam.util.Interval;
import org.rakam.util.MMapFile;
import org.rakam.util.NotImplementedException;
import org.rakam.util.json.JsonObject;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

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
                if (value == null) continue;
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
                            return ColumnStore.createColumnStore("/tmp/" + tableName + "/" + column, type);
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
        MMapFile storage;
        static int block_size = 32 * 1024 * 1024;
        static int index_block_size = 32 * 1024 * 1024;

        public ColumnStore(String path) {
            try {
                storage = new MMapFile(path + ".data", READ_WRITE, block_size);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        public void put(Object value) {
            try {
                add(value);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public abstract void add(Object value) throws IOException;

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
                    throw new UnsupportedOperationException("column type is not supported");
            }

        }
    }

    public static class FloatColumnStore extends ColumnStore {

        public FloatColumnStore(String name) {
            super(name);
        }

        @Override
        public void add(Object value) throws IOException {
            storage.ensureCapacity(4);
            storage.writeFloat((Float) value);
        }
    }

    public static class IntegerColumnStore extends ColumnStore {

        public IntegerColumnStore(String name) {
            super(name);
        }

        @Override
        public void add(Object value) throws IOException {
            storage.ensureCapacity(4);
            storage.writeInt((Integer) value);
        }
    }


    public static class LongColumnStore extends ColumnStore {

        public LongColumnStore(String name) {
            super(name);
        }

        @Override
        public void add(Object value) throws IOException {
            storage.ensureCapacity(8);
            storage.writeLong((Long) value);
        }
    }

    public static class StringColumnStore extends ColumnStore {
        private MMapFile indexStorage;

        public StringColumnStore(String path) {
            super(path);
            try {
                indexStorage = new MMapFile(path + ".index", READ_WRITE, index_block_size);
            } catch (IOException e) {
                throw new IllegalStateException();
            }
        }

        @Override
        public void add(Object value) throws IOException {
            indexStorage.ensureCapacity(4);
            indexStorage.writeInt(storage.position());
            storage.ensureCapacity(Short.MAX_VALUE);
            storage.writeUTF((String) value);
        }
    }

    public static class IntervalColumnPair implements SinkSerializable {
        public final int startTimestamp;
        public final String columnName;
        private static Charset charset = Charset.forName("UTF-8");

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof IntervalColumnPair)) return false;

            IntervalColumnPair that = (IntervalColumnPair) o;

            if (startTimestamp != that.startTimestamp) return false;
            if (!columnName.equals(that.columnName)) return false;
            if (!tableName.equals(that.tableName)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = startTimestamp;
            result = 31 * result + columnName.hashCode();
            result = 31 * result + tableName.hashCode();
            return result;
        }

        public final String tableName;

        public IntervalColumnPair(String tableName, String columnName, int startTimestamp) {
            this.startTimestamp = startTimestamp;
            this.columnName = columnName;
            this.tableName = tableName;
        }

        @Override
        public void writeTo(PrimitiveSink sink) {
            sink.putInt(startTimestamp);
            sink.putString(tableName, charset);
            sink.putString(columnName, charset);
        }
    }
}
