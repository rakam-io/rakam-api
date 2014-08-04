package org.rakam.cache.hazelcast.operations;

import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import org.rakam.cache.hazelcast.RakamDataSerializableFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Created by buremba <Burak Emre Kabakcı> on 21/07/14 06:40.
 */
public class AddAllStringSetOperation extends AbstractEntryProcessor<String, Set<String>> implements IdentifiedDataSerializable {


    private Collection<String> items;

    public AddAllStringSetOperation(Collection<String> items) {
        this.items = items;
    }

    public AddAllStringSetOperation() {

    }

    @Override
    public Object process(Map.Entry<String, Set<String>> entry) {
        Set<String> value = entry.getValue();
        if(value==null) {
            value = new ConcurrentSkipListSet(items);
        }else {
            value.addAll(items);
        }
        entry.setValue(value);
        return null;
    }

    @Override
    public int getFactoryId() {
        return RakamDataSerializableFactory.F_ID;
    }

    @Override
    public int getId() {
        return RakamDataSerializableFactory.ADD_ALL_STRING_SET_OPERATION;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {

        out.writeObject(items);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {

        items = in.readObject();
    }
}
