package org.rakam.stream.kume.service;

import org.rakam.kume.Cluster;
import org.rakam.kume.service.DistributedObjectServiceAdapter;
import org.rakam.stream.AverageCounter;

/**
 * Created by buremba <Burak Emre Kabakcı> on 30/12/14 22:29.
 */
public class AverageGCounterService extends DistributedObjectServiceAdapter<AverageGCounterService, AverageCounter> {

    public AverageGCounterService(Cluster.ServiceContext clusterContext, AverageCounter l, int replicationFactor) {
        super(clusterContext, l, replicationFactor);
    }

    public AverageGCounterService(Cluster.ServiceContext clusterContext, int replicationFactor) {
        super(clusterContext, new AverageCounter(), replicationFactor);
    }

    public void add(long sum) {
        sendToReplicas((service, ctx) -> service.value.add(sum, 1));
    }

    public void add(long sum, long count) {
        sendToReplicas((service, ctx) -> service.value.add(sum, count));
    }

    @Override
    protected boolean mergeIn(AverageCounter val) {
        if(val.getCount() > value.getCount()) {
            value = val;
            return true;
        }
        return false;
    }

    public static AverageCounter merge(AverageCounter val0, AverageCounter val1) {
        return val0.getCount() > val1.getCount() ? val0 : val1;
    }
}
