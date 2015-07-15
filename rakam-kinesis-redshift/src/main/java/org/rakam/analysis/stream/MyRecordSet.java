package org.rakam.analysis.stream;

import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.avro.generic.GenericRecord;

import java.util.List;
import java.util.stream.Collectors;

/**
* Created by buremba <Burak Emre Kabakcı> on 07/07/15 07:22.
*/
class MyRecordSet implements RecordSet {

    private final List<RakamMetadata.MyConnectorColumnHandle> columns;
    private final List<GenericRecord> events;
    private int idx = -1;

    public MyRecordSet(List<ConnectorColumnHandle> columns, List<GenericRecord> events) {
        this.columns = columns.stream().map(x -> ((RakamMetadata.MyConnectorColumnHandle) x)).collect(Collectors.toList());
        this.events = events;
    }

    @Override
    public List<Type> getColumnTypes() {
        return columns.stream().map(c -> c.getType()).collect(Collectors.toList());
    }

    @Override
    public RecordCursor cursor() {
        return new RecordCursor() {
            @Override
            public long getTotalBytes() {
                return 0;
            }

            @Override
            public long getCompletedBytes() {
                return 0;
            }

            @Override
            public long getReadTimeNanos() {
                return 0;
            }

            @Override
            public Type getType(int field) {
                return columns.get(field).getType();
            }

            @Override
            public boolean advanceNextPosition() {
                return events.size() - 1 > idx++;
            }

            @Override
            public boolean getBoolean(int field) {
                return (boolean) events.get(idx).get(field);
            }

            @Override
            public long getLong(int field) {
                return (long) events.get(idx).get(field);
            }

            @Override
            public double getDouble(int field) {
                return (double) events.get(idx).get(field);
            }

            @Override
            public Slice getSlice(int field) {
                return Slices.utf8Slice((String) events.get(idx).get(field));
            }

            @Override
            public boolean isNull(int field) {
                return events.get(idx).get(field) == null;
            }

            @Override
            public void close() {

            }
        };
    }
}
