package org.rakam.collection.kafka;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.airlift.log.Logger;
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
import org.rakam.util.HostAddress;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/02/15 00:01.
 */
@Singleton
public class KafkaOffsetManager {
    private final static Logger LOGGER = Logger.get(KafkaOffsetManager.class);
    private final KafkaSimpleConsumerManager consumerManager;
    private final KafkaConfig config;

    @Inject
    public KafkaOffsetManager(@Named("event.store.kafka") KafkaConfig config) {
        this.config = checkNotNull(config, "config is null");
        this.consumerManager = new KafkaSimpleConsumerManager();
    }

    public Map<String, Long> getOffset(String project, Set<String> collections) {
        return getTopicOffsets(collections.stream()
                .map(col -> project+"_"+col.toLowerCase()).collect(Collectors.toList()));
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
                LOGGER.debug(format("Adding Partition %s/%s", metadata.topic(), part.partitionId()));
                Broker leader = part.leader();
                if (leader == null) { // Leader election going on...
                    LOGGER.warn(format("No leader for partition %s/%s found!", metadata.topic(), part.partitionId()));
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
            LOGGER.warn(format("Offset response has error: %d", errorCode));
            throw new RakamException("could not fetch data from Kafka, error code is '" + errorCode + "'", 500);
        }

        long[] offsets = offsetResponse.offsets(topicName, partitionId);

        return offsets;
    }
}
