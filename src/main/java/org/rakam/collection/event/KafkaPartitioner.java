package org.rakam.collection.event;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * Created by buremba <Burak Emre Kabakcı> on 06/02/15 01:30.
 */
public class KafkaPartitioner implements Partitioner {
    private final VerifiableProperties props;

    public KafkaPartitioner(VerifiableProperties props) {
        this.props = props;
    }

    @Override
    public int partition(Object key, int numPartitions) {
        return 0;
    }
}
