/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.rakam.analysis.stream;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.avro.generic.GenericRecord;

import java.util.List;

public class AvroRecordCursor
        implements RecordCursor {

    private final int[] projections;
    private final List<Type> types;
    private GenericRecord record;
    private boolean ready;

    public AvroRecordCursor(List<Type> types, int[] projections) {
        this.projections = projections;
        this.types = types;
    }

    public void setRecord(GenericRecord record) {
        this.record = record;
        ready = true;
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
        return types.get(field);
    }

    @Override
    public boolean advanceNextPosition() {
        if (ready) {
            ready = false;
            return true;
        }
        return false;
    }

    @Override
    public boolean getBoolean(int field) {
        return (Boolean) record.get(projections[field]);
    }

    @Override
    public long getLong(int field) {
        return (Long) record.get(projections[field]);
    }

    @Override
    public double getDouble(int field) {
        return (Double) record.get(projections[field]);
    }

    @Override
    public Slice getSlice(int field) {
        return Slices.utf8Slice((String) record.get(projections[field]));
    }

    @Override
    public Object getObject(int field) {
        return record.get(projections[field]);
    }

    @Override
    public boolean isNull(int field) {
        return record.get(projections[field]) == null;
    }

    @Override
    public void close() {

    }
}
