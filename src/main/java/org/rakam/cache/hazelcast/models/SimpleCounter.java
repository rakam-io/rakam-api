package org.rakam.cache.hazelcast.models;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import org.rakam.cache.hazelcast.RakamDataSerializableFactory;

import java.io.IOException;

/**
 * Created by buremba <Burak Emre Kabakcı> on 20/07/14 06:38.
 */
public class SimpleCounter implements IdentifiedDataSerializable, Counter {
    private long value;

    public SimpleCounter(long i) {
        this.value = i;
    }

    public SimpleCounter() {

    }

    public void increment(long sum) {
        value += sum;
    }

    public void setValue(long l) {
       value = l;
    }


    @Override
    public int getFactoryId() {
        return RakamDataSerializableFactory.F_ID;
    }

    @Override
    public int getId() {
        return RakamDataSerializableFactory.COUNTER;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(getValue());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        value = in.readLong();
    }

    @Override
    public long getValue() {
        return value;
    }

    @Override
    public int compareTo(Object o) {
        if(o instanceof Counter)
            return (int) (this.getValue() - ((Counter) o).getValue());

        throw new IllegalArgumentException();
    }
}
