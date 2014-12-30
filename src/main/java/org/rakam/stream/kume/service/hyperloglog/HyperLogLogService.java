package org.rakam.stream.kume.service.hyperloglog;

import org.rakam.kume.Cluster;
import org.rakam.kume.service.DistributedObjectServiceAdapter;

/**
 * Created by buremba <Burak Emre Kabakcı> on 29/12/14 04:17.
 */
public class HyperLogLogService extends DistributedObjectServiceAdapter<HyperLogLogService, HLLWrapper> {
    public HyperLogLogService(Cluster.ServiceContext clusterContext, HLLWrapper value, int replicationFactor) {
        super(clusterContext, value, replicationFactor);
    }

    @Override
    protected boolean mergeIn(HLLWrapper val) {
        return false;
    }
}
