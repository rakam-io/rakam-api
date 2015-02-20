package org.rakam.realtime;

import com.facebook.presto.jdbc.internal.guava.collect.Maps;
import com.google.common.collect.ImmutableMap;
import com.google.inject.name.Named;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import org.rakam.collection.event.metastore.EventSchemaMetastore;
import org.rakam.config.KafkaConfig;
import org.rakam.kume.Cluster;
import org.rakam.kume.service.ringmap.RingMap;
import org.rakam.util.HostAddress;
import org.rakam.util.RakamException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Long.compare;
import static java.lang.Math.abs;
import static java.util.stream.Collectors.toMap;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/02/15 00:01.
 */
public class KafkaOffsetManager {
    final static Logger LOGGER = LoggerFactory.getLogger(RealTimeHttpService.class);
    private final KafkaSimpleConsumerManager consumerManager;
    private final KafkaConfig kafkaConfig;
    private final ScheduledExecutorService executor;
    private final RingMap<String, Map<String, Map<Long, Long>>> offsetMap;
    private final EventSchemaMetastore metastore;
    private final long timeoutValue;
    private final long updateInterval;

    @Inject
    public KafkaOffsetManager(@Named("event.store.kafka") KafkaConfig kafkaConfig, RealTimeConfig config, Cluster cluster, EventSchemaMetastore metastore) {
        this.kafkaConfig = checkNotNull(kafkaConfig, "kafkaConfig is null");

        RealTimeConfig realTimeConfig = checkNotNull(config, "config is null");
        this.timeoutValue = realTimeConfig.getTimeout().roundTo(TimeUnit.SECONDS);
        this.updateInterval = realTimeConfig.getUpdateInterval().roundTo(TimeUnit.SECONDS);

        this.metastore = checkNotNull(metastore, "metastore is null");
        this.offsetMap = checkNotNull(cluster, "cluster is null")
                .createOrGetService("kafka_offsets", bus -> new RingMap<>(bus, (v0, v1) -> v0, 2));

        this.consumerManager = new KafkaSimpleConsumerManager();
        this.executor = Executors.newSingleThreadScheduledExecutor();

        executor.scheduleAtFixedRate(this::updateOffsets, 0, updateInterval, TimeUnit.SECONDS);
    }

    public Map<String, Long> getOffsetOfCollections(String project) {
        long epochSecond = Instant.now().getEpochSecond();

        Map<String, Map<Long, Long>> collections = offsetMap.get(project).join();
        if (collections != null) {
            long requestedTime = (epochSecond / timeoutValue) * (timeoutValue - 1);

            return collections.entrySet().stream()
                    .collect(toMap(Map.Entry::getKey, e -> {
                        Map<Long, Long> value = e.getValue();
                        Long aLong = value.get(requestedTime);
                        return 0L;
//                        if (aLong != null) {
//                            return aLong;
//                        } else {
//                            return value.entrySet()
//                                    .stream()
//                                    .sorted((o1, o2) -> compare(abs(o1.getKey() - requestedTime), abs(o2.getKey() - requestedTime)))
//                                    .findFirst().get().getValue();
//                        }
                    }));
        }
        return null;
    }

    public Long getOffsetOfCollection(String project, String collection) {

        Map<String, Map<Long, Long>> map = offsetMap.get(project).join();
        Map<Long, Long> longLongMap = null;
        if (map != null) {
            longLongMap = map.get(collection);
        }

        if (longLongMap == null || map == null)
            return null;

        long epochSecond = Instant.now().getEpochSecond();
        long requestedTime = (epochSecond / timeoutValue) * timeoutValue;
        Long aLong = longLongMap.get(requestedTime);
        if(aLong == null) {
            return longLongMap.entrySet()
                        .stream()
                        .sorted((o1, o2) -> compare(abs(o1.getKey() - requestedTime), abs(o2.getKey() - requestedTime)))
                        .findFirst().get().getValue();
        } else {
            return aLong;
        }
    }


    private void updateOffsets() {
        List<String> allTopics = metastore.getAllCollections()
                .entrySet().stream()
                .flatMap(e -> e.getValue().stream().map(c -> e.getKey() + "_" + c))
                .collect(Collectors.toList());

        Map<String, Map<String, Long>> map = Maps.newHashMap();
        getTopicOffsets(allTopics).forEach((key, value) -> {
            String[] projectCollection = key.split("_", 2);
            map.computeIfAbsent(projectCollection[0], c -> new HashMap())
                    .put(projectCollection[1], value);
        });

        long now = (Instant.now().getEpochSecond() / updateInterval) * updateInterval;

        map.forEach((project, collectionOffsets) ->
                offsetMap.execute(project, (key, value) -> {
            Map<String, Map<Long, Long>> value1 = value.value();
            if(value1 == null) {
                Map<String, Map<Long, Long>> m = Maps.newHashMap();
                collectionOffsets.forEach((collection, offset) -> {
                    HashMap<Long, Long> off = Maps.newHashMap();
                    off.put(now, offset);
                    m.put(collection, off);
                });
                value.value(m);
            } else {
                collectionOffsets.forEach((collection, offset) ->
                        value1.computeIfAbsent(collection, c -> new HashMap<>()).put(now, offset));
            }

            return true;
        }));
    }

    private Map<String, Long> getTopicOffsets(List<String> topics) {
        ArrayList<HostAddress> nodes = new ArrayList<>(kafkaConfig.getNodes());
        Collections.shuffle(nodes);

        SimpleConsumer simpleConsumer = consumerManager.getConsumer(nodes.get(0));
        TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(topics);
        TopicMetadataResponse topicMetadataResponse = simpleConsumer.send(topicMetadataRequest);

        ImmutableMap.Builder<String, Long> builder = ImmutableMap.builder();

        for (TopicMetadata metadata : topicMetadataResponse.topicsMetadata()) {
            for (PartitionMetadata part : metadata.partitionsMetadata()) {
                LOGGER.debug("Adding Partition {}/{}", metadata.topic(), part.partitionId());
                Broker leader = part.leader();
                if (leader == null) { // Leader election going on...
                    LOGGER.warn("No leader for partition {}/{} found!", metadata.topic(), part.partitionId());
                } else {
                    HostAddress leaderHost = HostAddress.fromParts(leader.host(), leader.port());
                    SimpleConsumer leaderConsumer = consumerManager.getConsumer(leaderHost);

                    long offset = findAllOffsets(leaderConsumer, metadata.topic(), part.partitionId())[0];
                    builder.put(metadata.topic(), offset);
                }
            }
        }

        return builder.build();
    }

    private static long[] findAllOffsets(SimpleConsumer consumer, String topicName, int partitionId) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topicName, partitionId);

        // The API implies that this will always return all of the offsets. So it seems a partition can not have
        // more than Integer.MAX_VALUE-1 segments.
        //
        // This also assumes that the lowest value returned will be the first segment available. So if segments have been dropped off, this value
        // should not be 0.
        PartitionOffsetRequestInfo partitionOffsetRequestInfo = new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 10000);
        OffsetRequest offsetRequest = new OffsetRequest(ImmutableMap.of(topicAndPartition, partitionOffsetRequestInfo), kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());
        OffsetResponse offsetResponse = consumer.getOffsetsBefore(offsetRequest);

        if (offsetResponse.hasError()) {
            short errorCode = offsetResponse.errorCode(topicName, partitionId);
            LOGGER.warn("Offset response has error: %d", errorCode);
            throw new RakamException("could not fetch data from Kafka, error code is '" + errorCode + "'", 500);
        }

        long[] offsets = offsetResponse.offsets(topicName, partitionId);

        return offsets;
    }
}
