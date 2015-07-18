package org.rakam.analysis.stream;

import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.avro.generic.GenericRecord;
import org.rakam.collection.Event;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
* Created by buremba <Burak Emre Kabakcı> on 07/07/15 07:22.
*/
class EventRecordSet implements RecordSet {

    private final List<RakamMetadata.MyConnectorColumnHandle> columns;
    private final Iterator<Event> eventIterator;
    private GenericRecord event;

    public EventRecordSet(List<ConnectorColumnHandle> columns, Iterable<Event> eventIterator) {
        this.columns = columns.stream().map(x -> ((RakamMetadata.MyConnectorColumnHandle) x)).collect(Collectors.toList());
        this.eventIterator = eventIterator.iterator();
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
                if(eventIterator.hasNext()) {
                    event = eventIterator.next().properties();
                    return true;
                }else {
                    return false;
                }
            }

            @Override
            public boolean getBoolean(int field) {
                return (boolean) event.get(field);
            }

            @Override
            public long getLong(int field) {
                return (long) event.get(field);
            }

            @Override
            public double getDouble(int field) {
                return (double) event.get(field);
            }

            @Override
            public Slice getSlice(int field) {
                return Slices.utf8Slice((String) event.get(field));
            }

            @Override
            public boolean isNull(int field) {
                return event.get(field) == null;
            }

            @Override
            public void close() {

            }
        };
    }
}
