package org.rakam.analysis;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import org.rakam.analysis.model.AggregationRuleList;

/**
 * Created by buremba on 20/01/14.
 */
public class AggregationRuleListFactory implements DataSerializableFactory {
    public static final int ID = 1;

    @Override
    public IdentifiedDataSerializable create(int typeId) {
        return new AggregationRuleList();
    }
}
