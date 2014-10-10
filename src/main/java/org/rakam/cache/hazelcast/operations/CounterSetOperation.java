package org.rakam.cache.hazelcast.operations;

import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import org.rakam.cache.hazelcast.RakamDataSerializableFactory;
import org.rakam.cache.hazelcast.models.SimpleCounter;

import java.io.IOException;
import java.util.Map;

/**
 * Created by buremba <Burak Emre Kabakcı> on 30/09/14 17:10.
 */
public class CounterSetOperation extends AbstractEntryProcessor<String, SimpleCounter> implements IdentifiedDataSerializable {
    private long inc;

    public CounterSetOperation(long incrementBy) {
        this.inc = incrementBy;
    }

    public CounterSetOperation() {
    }

    @Override
    public Object process(Map.Entry<String, SimpleCounter> entry) {
        SimpleCounter value = entry.getValue();
        if (value == null) {
            value = new SimpleCounter(inc);
        } else {
            value.setValue(inc);
        }
        entry.setValue(value);
        return null;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(inc);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        inc = in.readLong();
    }

    @Override
    public int getFactoryId() {
        return RakamDataSerializableFactory.F_ID;
    }

    @Override
    public int getId() {
        return RakamDataSerializableFactory.COUNTER_SET_OPERATION;
    }

}
