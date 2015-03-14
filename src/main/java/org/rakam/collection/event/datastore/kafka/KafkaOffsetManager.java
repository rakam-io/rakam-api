package org.rakam.collection.event.datastore.kafka;

import com.facebook.presto.jdbc.internal.client.QueryError;
import com.facebook.presto.jdbc.internal.guava.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;
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
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.rakam.analysis.MaterializedView;
import org.rakam.analysis.TableStrategy;
import org.rakam.collection.event.metastore.EventSchemaMetastore;
import org.rakam.config.KafkaConfig;
import org.rakam.kume.util.FutureUtil;
import org.rakam.realtime.RealTimeHttpService;
import org.rakam.report.PrestoConfig;
import org.rakam.report.PrestoExecutor;
import org.rakam.report.metadata.ReportMetadataStore;
import org.rakam.util.HostAddress;
import org.rakam.util.RakamException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 15/02/15 00:01.
 */
public class KafkaOffsetManager implements Watcher {
    final static Logger LOGGER = LoggerFactory.getLogger(RealTimeHttpService.class);
    private final KafkaSimpleConsumerManager consumerManager;
    private final EventSchemaMetastore metastore;
    private final ReportMetadataStore reportMetadata;
    private final KafkaConfig config;
    private final PrestoExecutor prestoExecutor;
    private final PrestoConfig prestoConfig;
    private CuratorFramework zk;

    @Inject
    public KafkaOffsetManager(@Named("event.store.kafka") KafkaConfig config, PrestoConfig prestoConfig, PrestoExecutor prestoExecutor, EventSchemaMetastore metastore, ReportMetadataStore reportMetadata) {
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
                .flatMap(e -> e.getValue().stream().map(c -> e.getKey() + "_" + c))
                .collect(Collectors.toList());

        Map<String, List<MaterializedView>> views = reportMetadata.getAllMaterializedViews(TableStrategy.STREAM);

        Map<String, Long> topicOffsets = getTopicOffsets(allTopics);
        FutureUtil.MultipleFutureListener listeners = new FutureUtil.MultipleFutureListener(topicOffsets.size());

        topicOffsets.forEach((key, value) -> {
            String[] projectCollection = key.split("_", 2);

            Long offset;
            try {
                byte[] data = zk.getData().forPath("/collectionOffsets/" + key);
                offset = Longs.fromByteArray(data);
            } catch (Exception e) {
                offset = 0L;
                LOGGER.error(format("Couldn't create get offset from Zookeeper for collection %s", key), e);
                return;
            }

            if(value.equals(offset))
                return;

            String query = buildQuery(projectCollection[0], projectCollection[1], offset, value, views.get(projectCollection[0]));

            prestoExecutor.executeQuery(query).thenAccept(result -> {
                System.out.println(result);
                QueryError error = result.getError();
                if (error != null) {
                    String tableName = projectCollection[0].toLowerCase() + "." + projectCollection[1].toLowerCase();
                    String notExistMessage = format("Table '%s' does not exist", "%s.%s",
                            prestoConfig.getColdStorageConnector(), tableName);
                    // this is a workaround until presto starts to use sql error codes.
                    if (error.getFailureInfo().getMessage().equals(notExistMessage)) {
                        String format = format("CREATE TABLE %s.%s AS SELECT * FROM %s.%s",
                                prestoConfig.getColdStorageConnector(), tableName,
                                prestoConfig.getHotStorageConnector(), tableName);
                        prestoExecutor.executeQuery(format).thenAccept(result1 -> {
                            if (result1.getError() != null)
                                LOGGER.error(format("Couldn't create cold storage table %s", tableName), result1.getError());
                            listeners.increment();
                            updateOffset(key, value);
                        });
//                    try {
//                        pool.getPrestoDriver().createStatement().execute(query);
//                    } catch (SQLException e1) {
//                        LOGGER.error(format("Couldn't processed messages (%d - %d) in Kafka broker.", 0, 0), e);
//                    }
                    } else {
                        LOGGER.error(format("Couldn't processed messages (%d - %d) in Kafka broker.", 0, 0), error);
                        listeners.increment();
                        updateOffset(key, value);
                    }
                } else {
                    listeners.increment();
                    updateOffset(key, value);
                }
            });
        });

        listeners.get().thenRun(() -> LOGGER.error("successfully processed batches in {}", Duration.between(start, Instant.now())));
    }

    private void updateOffset(String key, Long value) {
        try {
            zk.setData().forPath("/collectionOffsets/" + key, Longs.toByteArray(value));
        } catch (KeeperException.NoNodeException e) {
            try {
                zk.setData().forPath("/collectionOffsets/" + key, Longs.toByteArray(value));
            } catch (Exception e1) {
                throw Throwables.propagate(e1);
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private String buildQuery(String project, String collection, long startOffset, long endOffset, List<MaterializedView> reports) {
        StringBuilder builder = new StringBuilder();
        builder.append(format("WITH %3$s AS (SELECT * FROM %1$s.%2$s.%3$s WHERE _offset > %4$d AND _offset < %5$d) ",
                prestoConfig.getHotStorageConnector(), project, collection, startOffset, endOffset));

        int i = 0;
        builder.append(format(", stream as (INSERT INTO %1$s.%2$s.%3$s SELECT * FROM %3$s)",
                prestoConfig.getColdStorageConnector(), project, collection));
        for (MaterializedView report : reports.stream()
                .filter(p -> p.collections.contains(collection)).collect(Collectors.toList())){
            builder.append(format(", view%d as (INSERT INTO %s (%s)) ", i++, report.name, report.query));
        }
        builder.append(" SELECT * FROM stream");
        IntStream.range(0, i).mapToObj(id -> " UNION SELECT * FROM view"+id).forEach(builder::append);
        return builder.toString();
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

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }
}
