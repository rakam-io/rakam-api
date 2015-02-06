package org.rakam.plugin.realtime;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import org.rakam.kume.Cluster;
import org.rakam.kume.service.ringmap.RingMap;
import org.rakam.model.Event;
import org.rakam.plugin.EventProcessor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/02/15 13:41.
 */
public class RealTimeEventProcessor implements EventProcessor {
    RingMap<String, ObjectNode> db;
    ConfigService config;

    private List<String> m_replicaBrokers = new ArrayList<String>();

    @Inject
    public RealTimeEventProcessor(Cluster cluster) {
//        db = cluster.createOrGetService("realtime",
//                bus -> new RingMap<>(bus, (first, second) -> {
//                    first.setAll(second);
//                    return first;
//                }, 1));
//        config = cluster.createOrGetService("realtime", bus -> new ConfigService(bus));

//        PartitionMetadata metadata = findLeader(a_seedBrokers, a_port, a_topic, a_partition);
//        if (metadata == null) {
//            System.out.println("Can't find metadata for Topic and Partition. Exiting");
//            return;
//        }
//        if (metadata.leader() == null) {
//            System.out.println("Can't find Leader for Topic and Partition. Exiting");
//            return;
//        }
//        String leadBroker = metadata.leader().host();
//        String clientName = "Client_" + a_topic + "_" + a_partition;
//
//        SimpleConsumer consumer = new SimpleConsumer(leadBroker, a_port, 100000, 64 * 1024, clientName);
//        long readOffset = getLastOffset(consumer,a_topic, a_partition, kafka.api.OffsetRequest.EarliestTime(), clientName);
//
//        int numErrors = 0;
//        while (a_maxReads > 0) {
//            if (consumer == null) {
//                consumer = new SimpleConsumer(leadBroker, a_port, 100000, 64 * 1024, clientName);
//            }
//            FetchRequest req = new FetchRequestBuilder()
//                    .clientId(clientName)
//                    .addFetch(a_topic, a_partition, readOffset, 100000) // Note: this fetchSize of 100000 might need to be increased if large batches are written to Kafka
//                    .build();
//            FetchResponse fetchResponse = consumer.fetch(req);
//
//            if (fetchResponse.hasError()) {
//                numErrors++;
//                // Something went wrong!
//                short code = fetchResponse.errorCode(a_topic, a_partition);
//                System.out.println("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
//                if (numErrors > 5) break;
//                if (code == ErrorMapping.OffsetOutOfRangeCode())  {
//                    // We asked for an invalid offset. For simple case ask for the last element to reset
//                    readOffset = getLastOffset(consumer,a_topic, a_partition, kafka.api.OffsetRequest.LatestTime(), clientName);
//                    continue;
//                }
//                consumer.close();
//                consumer = null;
//                leadBroker = findNewLeader(leadBroker, a_topic, a_partition, a_port);
//                continue;
//            }
//            numErrors = 0;
//
//            long numRead = 0;
//            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(a_topic, a_partition)) {
//                long currentOffset = messageAndOffset.offset();
//                if (currentOffset < readOffset) {
//                    System.out.println("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
//                    continue;
//                }
//                readOffset = messageAndOffset.nextOffset();
//                ByteBuffer payload = messageAndOffset.message().payload();
//
//                byte[] bytes = new byte[payload.limit()];
//                payload.get(bytes);
//                System.out.println(String.valueOf(messageAndOffset.offset()) + ": " + new String(bytes, "UTF-8"));
//                numRead++;
//                a_maxReads--;
//            }
//
//            if (numRead == 0) {
//                try {
//                    Thread.sleep(1000);
//                } catch (InterruptedException ie) {
//                }
//            }
//        }
//        if (consumer != null) consumer.close();

    }

    @Override
    public void process(Event event) {
//        for (RealtimeRule realtimeRule : config.getRules(event.project())) {
//            realtimeRule.segment();
//            Map<String, ObjectNode> db;
//        }
//        db.merge(event.user, event.properties, )
//        event.user;
//
//        FetchRequest req = new FetchRequestBuilder()
//                .clientId("test")
//                .addFetch(a_topic, a_partition, readOffset, 100000)
//                .build();
//        FetchResponse fetchResponse = consumer.fetch(req);
    }

    private static ConsumerConfig createConsumerConfig(String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", "127.0.0.1");
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        return new ConsumerConfig(props);
    }

    public static long getLastOffset(SimpleConsumer consumer, String topic, int partition,
                                     long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(),clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition) );
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }

    private PartitionMetadata findLeader(List<String> a_seedBrokers, int a_port, String a_topic, int a_partition) {
        PartitionMetadata returnMetaData = null;
        loop:
        for (String seed : a_seedBrokers) {
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(seed, a_port, 100000, 64 * 1024, "leaderLookup");

                List<String> topics = Collections.singletonList(a_topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

                List<TopicMetadata> metaData = resp.topicsMetadata();

                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == a_partition) {
                            returnMetaData = part;
                            break loop;
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("Error communicating with Broker [" + seed + "] to find Leader for [" + a_topic
                        + ", " + a_partition + "] Reason: " + e);
            } finally {
                if (consumer != null) consumer.close();
            }
        }
        if (returnMetaData != null) {
            m_replicaBrokers.clear();
            for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
                m_replicaBrokers.add(replica.host());
            }
        }
        return returnMetaData;
    }
}
