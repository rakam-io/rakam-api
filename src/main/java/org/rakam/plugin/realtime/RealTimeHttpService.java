package org.rakam.plugin.realtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Singleton;
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
import org.rakam.server.http.HttpService;
import org.rakam.server.http.annotations.JsonRequest;
import org.rakam.util.HostAddress;
import org.rakam.util.RakamException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.Path;
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
 * Created by buremba <Burak Emre Kabakcı> on 02/02/15 14:30.
 */
@Singleton
@Path("/realtime")
public class RealTimeHttpService implements HttpService {
    final static Logger LOGGER = LoggerFactory.getLogger(RealTimeHttpService.class);
    private final KafkaSimpleConsumerManager consumerManager;
    private final KafkaConfig config;
    private final ScheduledExecutorService executor;

    @Inject
    public RealTimeHttpService(@Named("event.store.kafka") KafkaConfig config, EventSchemaMetastore metastore) {
        this.config = checkNotNull(config, "config is null");
        this.consumerManager = new KafkaSimpleConsumerManager();
        this.executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(() -> {
            List<String> allTopics = metastore.getAllCollections()
                    .entrySet().stream()
                    .map(e -> e.getKey() + "_" + e.getValue())
                    .collect(Collectors.toList());
            Map<String, Long> topicOffsets = getTopicOffsets(allTopics);

        }, 0, 5, TimeUnit.SECONDS);

    }

    @JsonRequest
    @Path("/")
    public JsonNode getWidget(JsonNode json) {
        JsonNode project = json.get("project");
        return project;
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
