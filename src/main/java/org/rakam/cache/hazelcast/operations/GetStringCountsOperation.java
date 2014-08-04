package org.rakam.cache.hazelcast.operations;

import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import org.rakam.cache.hazelcast.RakamDataSerializableFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Created by buremba <Burak Emre Kabakcı> on 22/07/14 03:13.
 */
public class GetStringCountsOperation extends AbstractEntryProcessor<String, Set<String>> implements IdentifiedDataSerializable {
    private Integer limit;

    public GetStringCountsOperation(Integer limit) {
        this.limit = limit;
    }

    public GetStringCountsOperation() {
    }

    @Override
    public int getFactoryId() {
        return RakamDataSerializableFactory.F_ID;
    }

    @Override
    public int getId() {
        return RakamDataSerializableFactory.GET_STRING_COUNTS_OPERATION;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(limit);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        limit = in.readInt();
    }

    @Override
    public Object process(Map.Entry<String, Set<String>> entry) {
        return entry.getValue().size();
    }
}
