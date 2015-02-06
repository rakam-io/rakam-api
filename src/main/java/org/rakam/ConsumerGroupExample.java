package org.rakam;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerGroupExample {
    private final ConsumerConnector consumer;
    private final String topic;
    private  ExecutorService executor;

    public ConsumerGroupExample(String a_zookeeper, String a_groupId, String a_topic) {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(a_zookeeper, a_groupId));
        this.topic = a_topic;
    }

    public void shutdown() {
        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();
    }

    public void run(int a_numThreads) {
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, a_numThreads);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        executor = Executors.newFixedThreadPool(a_numThreads);

        for (final KafkaStream<byte[], byte[]> stream : streams) {
            executor.submit(() -> new ConsumerTest(stream));
        }
    }

    private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        return new ConsumerConfig(props);
    }

    public static void main(String[] args) {
        String zooKeeper = "127.0.0.1:2181";
        String groupId = "ingestion";
        String topic = "twitter_feed";
        int threads = 4;

        ConsumerGroupExample example = new ConsumerGroupExample(zooKeeper, groupId, topic);
        example.run(threads);

        try {
            Thread.sleep(10000);
        } catch (InterruptedException ie) {

        }
        example.shutdown();

//        while(true) {
//            try {
//                eventCollector.submitEvent(new JsonObject()
//                        .put("project", "test")
//                        .put("name", "naber")
//                        .put("properties",
//                                new JsonObject()
//                                        .put("ali", "veli" + new Random().nextInt())
//                                        .put("naber", new Random().nextInt())));
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
    }

    private static class ConsumerTest {
        public ConsumerTest(KafkaStream<byte[], byte[]> stream) {

            ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
            while(iterator.hasNext()) {
                MessageAndMetadata<byte[], byte[]> next = iterator.next();
                System.out.println(next);
            }
        }
    }
}
