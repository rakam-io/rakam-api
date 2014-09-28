package org.rakam.analysis;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import org.rakam.cache.hazelcast.RakamDataSerializableFactory;
import org.rakam.cache.hazelcast.models.Counter;

import java.io.IOException;

/**
 * Created by buremba <Burak Emre Kabakcı> on 20/07/14 06:38.
 */
public class AverageCounter implements IdentifiedDataSerializable, Counter {
    protected long count;
    protected long sum;

    public AverageCounter(long sum, long count) {
        this.count = count;
        this.sum = sum;
    }

    public AverageCounter() {}

    public long getValue() {
        return count==0 ? 0 : sum/count;
    }

    public long getSum() {
        return sum;
    }


    public long getCount() {
        return count;
    }

    @Override
    public int getFactoryId() {
        return RakamDataSerializableFactory.F_ID;
    }

    @Override
    public int getId() {
        return RakamDataSerializableFactory.AVERAGE_COUNTER;
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
    public int compareTo(Object o) {
        if(o instanceof Counter)
         return (int) (this.getValue()- ((Counter) o).getValue());

        throw new IllegalArgumentException();
    }

    public void add(long sum, long count) {
        this.sum += sum;
        this.count += count;
    }
}
