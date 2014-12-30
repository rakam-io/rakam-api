package org.rakam.stream.kume.service;

import org.rakam.kume.Cluster;
import org.rakam.kume.service.DistributedObjectServiceAdapter;
import org.rakam.kume.service.ringmap.MapMergePolicy;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by buremba <Burak Emre Kabakcı> on 30/12/14 22:57.
 */
public class MapService<K, V> extends DistributedObjectServiceAdapter<MapService<K, V>, Map<K, V>> {

    private final MapMergePolicy<V> mergePolicy;

    public MapService(Cluster.ServiceContext clusterContext, Supplier<Map<K, V>> value, MapMergePolicy<V> mergePolicy, int replicationFactor) {
        super(clusterContext, value.get(), replicationFactor);
        this.mergePolicy = mergePolicy;
    }

    public void put(K key, V value) {
        sendToReplicas((service, ctx) -> service.value.put(key, value));
    }

    public void apply(K key, Consumer<V> op) {
        sendToReplicas((service, ctx) -> op.accept(service.value.get(key)));
    }

    public int size() {
        return askReplicas((service, ctx) -> service.value.size(), Integer.class)
                .mapToInt(future -> future.join()).max().getAsInt();
    }

    public CompletableFuture<List<Map.Entry<K, V>>> sortAndGet(Comparator<Map.Entry<K, V>> comparator, int limit) {
       return askRandomReplica(
               (service, ctx) -> {
                   Stream<Map.Entry<K, V>> stream = service.value.entrySet().stream().sorted(comparator);
                   ctx.reply(stream.limit(limit).collect(Collectors.toList()));
               });
    }

    public CompletableFuture<List<Map.Entry<K, V>>> sortAndGet(int limit) {
       return askRandomReplica(
                (service, ctx) -> {
                    Stream<Map.Entry<K, V>> stream = service.value.entrySet().stream();
                    ctx.reply(stream.limit(limit).collect(Collectors.toList()));
                });
    }

    @Override
    protected boolean mergeIn(Map<K, V> val) {
        // TODO: consider using lwwregister for map values.
        int size = value.size();
        val.forEach((k, v) -> value.merge(k, v, (cKey, cValue) -> mergePolicy.merge(cValue, v)));
        return value.size() == size;
    }
}
