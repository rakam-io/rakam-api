package org.rakam.collection.kafka;

import com.facebook.presto.jdbc.internal.guava.collect.Lists;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;
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
import org.apache.curator.framework.CuratorFramework;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.report.PrestoConfig;
import org.rakam.report.PrestoQueryExecutor;
import org.rakam.report.QueryError;
import org.rakam.util.HostAddress;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/02/15 00:01.
 */
@Singleton
public class KafkaOffsetManager {
    private final static Logger LOGGER = Logger.getLogger(KafkaOffsetManager.class);
    private final KafkaSimpleConsumerManager consumerManager;
    private final Metastore metastore;
    private final QueryMetadataStore reportMetadata;
    private final KafkaConfig config;
    private final PrestoQueryExecutor prestoExecutor;
    private final PrestoConfig prestoConfig;
    private CuratorFramework zk;

    @Inject
    public KafkaOffsetManager(@Named("event.store.kafka") KafkaConfig config, PrestoConfig prestoConfig, PrestoQueryExecutor prestoExecutor, Metastore metastore, QueryMetadataStore reportMetadata) {
        this.reportMetadata = checkNotNull(reportMetadata, "reportMetadata is null");
        this.prestoExecutor = checkNotNull(prestoExecutor, "prestoExecutor is null");
        this.config = checkNotNull(config, "config is null");
        this.prestoConfig = checkNotNull(prestoConfig, "prestoConfig is null");

        this.metastore = checkNotNull(metastore, "metastore is null");

        this.consumerManager = new KafkaSimpleConsumerManager();
    }

    void setZookeeper(CuratorFramework zk) {
        this.zk = zk;
    }

    void updateOffsets() {
        Instant start = Instant.now();
        List<String> allTopics = metastore.getAllCollections()
                .entrySet().stream()
                .flatMap(e -> e.getValue().stream().map(c -> e.getKey() + "_" + c.toLowerCase()))
                .collect(Collectors.toList());

        Map<String, List<ContinuousQuery>> views = reportMetadata.getAllContinuousQueries().stream()
                .collect(Collectors.groupingBy(k -> k.project));

        Map<String, Long> topicOffsets = getTopicOffsets(allTopics);
        List<CompletableFuture> futures = Lists.newArrayList();

        topicOffsets.forEach((key, finalOffset) -> {
            String[] projectCollection = key.split("_", 2);

            long colOffset;
            try {
                byte[] data = zk.getData().forPath("/collectionOffsets/" + key);
                colOffset = Longs.fromByteArray(data);
            } catch (KeeperException.NoNodeException e) {
                colOffset = 0;
            } catch (Exception e) {
                LOGGER.error(format("Couldn't create get offset from Zookeeper for collection %s", key), e);
                return;
            }
            final long offset = colOffset;

            if(finalOffset.equals(offset)) {
                return;
            }

            String query = buildQuery(projectCollection[0], projectCollection[1], offset, finalOffset, views.get(projectCollection[0]));

            CompletableFuture<Void> voidCompletableFuture = prestoExecutor.executeRawQuery(query).getResult().thenAccept(result -> {
                QueryError error = result.getError();
                if (error != null) {
                    String tableName = projectCollection[0].toLowerCase() + "." + projectCollection[1].toLowerCase();
                    String notExistMessage = format("Table '%s.%s' does not exist",
                            prestoConfig.getColdStorageConnector(), tableName);

                    // A workaround until presto starts to use sql error codes.
                    if (error.message.equals(notExistMessage)) {
                        String format = format("CREATE TABLE %s.%s AS SELECT * FROM %s.%s",
                                prestoConfig.getColdStorageConnector(), tableName,
                                prestoConfig.getHotStorageConnector(), tableName);
                        prestoExecutor.executeRawQuery(format).getResult().thenAccept(result1 -> {
                            if (result1.getError() != null) {
                                LOGGER.error(format("Couldn't create cold storage table %s: %s", tableName, result1.getError()));
                            } else {
                                prestoExecutor.executeRawQuery(query).getResult().thenAccept(res -> {
                                    if (res.getError() != null) {
                                        failSafe(key, offset, finalOffset, error);
                                    } else {
                                        successfullyProcessed(key, finalOffset);
                                    }

                                });
                            }
                        });

                    } else {
                        failSafe(key, offset, finalOffset, error);
                    }
                } else {
                    successfullyProcessed(key, finalOffset);
                }
            });
            futures.add(voidCompletableFuture);
        });

        CompletableFuture.allOf(futures.stream().toArray(CompletableFuture[]::new)).join();
        LOGGER.info(format("successfully processed batches in %s", Duration.between(start, Instant.now())));
    }

    private void failSafe(String zkKey, long value, long finalOffset, QueryError error) {
        LOGGER.error(format("Couldn't processed messages (%d - %d) in Kafka broker: %s", value, finalOffset, error));
        updateOffset(zkKey, finalOffset);
    }

    private void successfullyProcessed(String zkKey, long finalOffset) {
        updateOffset(zkKey, finalOffset);
    }

    private void updateOffset(String key, Long value) {
        try {
            zk.setData().forPath("/collectionOffsets/" + key, Longs.toByteArray(value));
        } catch (KeeperException.NoNodeException e) {
            try {
                zk.create().forPath("/collectionOffsets/" + key, Longs.toByteArray(value));
            } catch (Exception e1) {
                throw Throwables.propagate(e1);
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private String buildQuery(String project, String collection, long startOffset, long endOffset, List<ContinuousQuery> reports) {
        StringBuilder builder = new StringBuilder();
        builder.append(format("WITH stream AS (SELECT * FROM %1$s.%2$s.%3$s WHERE _offset > %4$d AND _offset < %5$d) ",
                prestoConfig.getHotStorageConnector(), project, collection, startOffset, endOffset));

        int i = 0;
        builder.append(format(", batch as (INSERT INTO %1$s.%2$s.%3$s SELECT * FROM %3$s)",
                prestoConfig.getColdStorageConnector(), project, collection));
        if(reports != null) {
            List<ContinuousQuery> queriesForCollection = reports.stream()
                    .filter(p -> p.collections.contains(collection)).collect(Collectors.toList());
            for (ContinuousQuery report : queriesForCollection) {
                builder.append(format(", view%d as (INSERT INTO %s.%s.%s (%s)) ",
                        i++, prestoConfig.getColdStorageConnector(), project, report.tableName, report.query));
            }
        }
        builder.append(" SELECT * FROM batch");
        IntStream.range(0, i).mapToObj(id -> " UNION SELECT * FROM view"+id).forEach(builder::append);
        return builder.toString();
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
