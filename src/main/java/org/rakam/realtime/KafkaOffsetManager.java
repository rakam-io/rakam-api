package org.rakam.realtime;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Table;
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
import org.rakam.util.NotExistsException;
import org.rakam.util.RakamException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/02/15 00:01.
 */
public class KafkaOffsetManager {
    final static Logger LOGGER = LoggerFactory.getLogger(RealTimeHttpService.class);
    private final KafkaSimpleConsumerManager consumerManager;
    private final KafkaConfig config;
    private final ScheduledExecutorService executor;
    private final RingMap<Long, Table<String, String, Long>> offsetMap;
    private final EventSchemaMetastore metastore;

    @Inject
    public KafkaOffsetManager(@Named("event.store.kafka") KafkaConfig config, Cluster cluster, EventSchemaMetastore metastore) {
        this.config = checkNotNull(config, "config is null");
        this.metastore = checkNotNull(metastore, "metastore is null");
        this.offsetMap = checkNotNull(cluster, "cluster is null")
                .createOrGetService("kafka_offsets", bus -> new RingMap<>(bus, (v0, v1) -> v0, 2));

        this.consumerManager = new KafkaSimpleConsumerManager();
        this.executor = Executors.newSingleThreadScheduledExecutor();

        executor.scheduleAtFixedRate(this::periodicOffsetFetcher, 0, 5, TimeUnit.SECONDS);
    }

    public Map<String, Long> getOffsetOfCollections(String project) {
        long epochSecond = Instant.now().getEpochSecond();

        return offsetMap.get((epochSecond / 45) * 45).join()
                .rowMap().get(project);
    }

    public long getOffsetOfCollection(String project, String collection) {
        long epochSecond = Instant.now().getEpochSecond();

        Long aLong = offsetMap.get((epochSecond / 45) * 45).join().get(project, collection);
        if(aLong == null) {
            throw new NotExistsException("collection doesn't exists");
        }
        return aLong;
    }


    private void periodicOffsetFetcher() {
        List<String> allTopics = metastore.getAllCollections()
                .entrySet().stream()
                .map(e -> e.getKey() + "_" + e.getValue())
                .collect(Collectors.toList());

        Table<String, String, Long> table = HashBasedTable.create();
        getTopicOffsets(allTopics).forEach((key, value) -> {
            String[] projectCollection = key.split("_", 2);
            table.put(projectCollection[0], projectCollection[1], value);
        });

        long epochSecond = Instant.now().getEpochSecond();
        offsetMap.put((epochSecond / 45)*45, table);
    }

    private Map<String, Long> getTopicOffsets(List<String> topics) {
        ArrayList<HostAddress> nodes = new ArrayList<>(config.getNodes());
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
                }
                else {
                    HostAddress leaderHost = HostAddress.fromParts(leader.host(), leader.port());
                    SimpleConsumer leaderConsumer = consumerManager.getConsumer(leaderHost);

                    long offset = findAllOffsets(leaderConsumer, metadata.topic(), part.partitionId())[0];
                    builder.put(metadata.topic(), offset);
                }
            }
        }

        return  builder.build();
    }

    private static long[] findAllOffsets(SimpleConsumer consumer, String topicName, int partitionId)
    {
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
