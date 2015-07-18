package org.rakam.analysis.stream;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.map.AbstractEntryProcessor;
import org.rakam.analysis.stream.processor.Processor;
import org.rakam.util.Tuple;

import java.util.Map;

/**
 * Created by buremba <Burak Emre Kabakcı> on 17/07/15 01:18.
 */
public class GetSchemaEntryProcessor extends AbstractEntryProcessor<Tuple<String, String>, Processor> implements HazelcastInstanceAware {
    @Override
    public Object process(Map.Entry<Tuple<String, String>, Processor> entry) {
        return entry.getValue().getSchema();
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {

    }
}
