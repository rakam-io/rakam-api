package org.rakam.cache.hazelcast.operations;

import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import org.rakam.cache.hazelcast.RakamDataSerializableFactory;
import org.rakam.cache.hazelcast.models.AverageCounter;

import java.io.IOException;
import java.util.Map;

/**
 * Created by buremba <Burak Emre Kabakcı> on 21/07/14 00:48.
 */
public class AverageCounterIncrementOperation extends AbstractEntryProcessor<String, AverageCounter> implements IdentifiedDataSerializable {
    private long sum;
    private long count;

    public AverageCounterIncrementOperation(long sum, long count) {
        this.sum = sum;
        this.count = count;
    }

    public AverageCounterIncrementOperation() {
    }

    @Override
    public Object process(Map.Entry<String, AverageCounter> entry) {
        AverageCounter value = entry.getValue();
        if(value==null) {
            value = new AverageCounter(sum, count);
        }else {
            value.add(sum, count);
        }
        entry.setValue(value);
        return null;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(sum);
        out.writeLong(count);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        sum = in.readLong();
        count = in.readLong();
    }

    @Override
    public int getFactoryId() {
        return RakamDataSerializableFactory.F_ID;
    }

    @Override
    public int getId() {
        return RakamDataSerializableFactory.AVERAGE_COUNTER_INCREMENT_OPERATION;
    }
}
