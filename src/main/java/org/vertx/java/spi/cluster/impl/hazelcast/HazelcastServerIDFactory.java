package org.vertx.java.spi.cluster.impl.hazelcast;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

/**
 * Created by buremba on 30/03/14.
 */
public class HazelcastServerIDFactory implements DataSerializableFactory {


    @Override
    public IdentifiedDataSerializable create(int typeId) {
        if (typeId == 0)
            return new HazelcastServerID();
        else
            throw new IllegalStateException("HazelcastServerID instance couldn't identified");
    }
}
