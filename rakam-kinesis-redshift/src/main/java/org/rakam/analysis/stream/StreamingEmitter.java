package org.rakam.analysis.stream;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.rakam.analysis.DynamoDBContinuousQueryService;
import org.rakam.analysis.IEmitter;
import org.rakam.collection.Event;
import org.rakam.plugin.ContinuousQuery;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by buremba <Burak Emre Kabakcı> on 07/07/15 03:17.
 */
public class StreamingEmitter implements IEmitter<Event> {
    private final DynamoDBContinuousQueryService service;

    public StreamingEmitter(DynamoDBContinuousQueryService service) {
        this.service = service;
    }

    @Override
    public List<Event> emit(List<Event> buffer) throws IOException {
        buffer.stream().collect(Collectors.groupingBy(x -> x.project()))
                .forEach((project, events) -> {
                    List<ContinuousQuery> list = service.list(project);
                    for (ContinuousQuery continuousQuery : list) {
                        service.updateReport(continuousQuery, events.stream().map(e -> e.properties()).collect(Collectors.toList()));
                    }
                });

        return null;
    }

    @Override
    public void fail(List<Event> records) {

    }

    @Override
    public void shutdown() {

    }

    public static class Cursor implements RecordCursor {

        private final GenericRecord record;
        private final List<Schema.Field> schema;

        public Cursor(GenericRecord record) {
            this.record = record;
            this.schema = record.getSchema().getFields();
        }

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
            return VarcharType.VARCHAR;
        }

        @Override
        public boolean advanceNextPosition() {
            return false;
        }

        @Override
        public boolean getBoolean(int field) {
            return (boolean) record.get(field);
        }

        @Override
        public long getLong(int field) {
            return 0;
        }

        @Override
        public double getDouble(int field) {
            return 0;
        }

        @Override
        public Slice getSlice(int field) {
            Object value = record.get(field);
            if (value instanceof byte[]) {
                return Slices.wrappedBuffer((byte[]) value);
            }
            if (value instanceof String) {
                return Slices.utf8Slice((String) value);
            }
            throw new IllegalArgumentException("Field " + field + " is not a String, but is a " + value.getClass().getName());
        }

        @Override
        public boolean isNull(int field) {
            return false;
        }

        @Override
        public void close() {

        }
    }


}
