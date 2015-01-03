package org.rakam.stream.kume.service;

import org.rakam.kume.Cluster;
import org.rakam.kume.service.DistributedObjectServiceAdapter;
import org.rakam.kume.service.ringmap.MapMergePolicy;
import org.rakam.util.Interval;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by buremba <Burak Emre Kabakcı> on 31/12/14 02:13.
 */
// keys are compressed timestamp values, values are stream data
public class TimeSeriesValue<V> extends DistributedObjectServiceAdapter<TimeSeriesValue<V>, List<V>> {
    private final MapMergePolicy<V> mergePolicy;
    final int firstTimestamp;
    final Interval interval;

    public TimeSeriesValue(Cluster.ServiceContext clusterContext, MapMergePolicy<V> mergePolicy, int replicationFactor, Interval interval) {
        super(clusterContext, new ArrayList<>(), replicationFactor);
        this.mergePolicy = mergePolicy;
        this.interval = interval;
        this.firstTimestamp = interval.spanCurrent().current();
    }


    public void put(int timestamp, V value) {
        int index = (timestamp - firstTimestamp)/interval.spanCurrent().current();
        sendToReplicas((service, ctx) -> service.value.set(index, value));
    }

    public void apply(int timestamp, Consumer<V> op) {
        int index = (timestamp - firstTimestamp)/interval.spanCurrent().current();
        sendToReplicas((service, ctx) -> op.accept(service.value.get(index)));
    }

    public CompletableFuture<V> get(int timestamp) {
        int index = (timestamp - firstTimestamp)/interval.spanCurrent().current();
        return askRandomReplica((service, ctx) -> service.value.get(index));
    }

    public <R> CompletableFuture<R> get(int timestamp, Function<V, R> func) {
        int index = (timestamp - firstTimestamp)/interval.spanCurrent().current();
        return askRandomReplica((service, ctx) -> func.apply(service.value.get(index)));
    }

//    public int size() {
//        return askReplicas((service, ctx) -> service.value.size(), Integer.class)
//                .mapToInt(future -> future.join()).max().getAsInt();
//    }

//    public CompletableFuture<List<Map.Entry<Integer, V>>> sortAndGet(Comparator<Map.Entry<Integer, V>> comparator, int limit) {
//        return askRandomReplica(
//                (service, ctx) -> {
//                    Stream<Map.Entry<K, V>> stream = service.valuestream().sorted(comparator);
//                    ctx.reply(stream.limit(limit).collect(Collectors.toList()));
//                });
//    }

//    public CompletableFuture<List<Map.Entry<Integer, V>>> sortAndGet(int limit) {
//        return askRandomReplica(
//                (service, ctx) -> {
//                    Stream<Map.Entry<K, V>> stream = service.value.entrySet().stream();
//                    ctx.reply(stream.limit(limit).collect(Collectors.toList()));
//                });
//    }

    @Override
    protected boolean mergeIn(List<V> val) {
        // TODO: consider using lwwregister for map values.
        return false;
    }
}

