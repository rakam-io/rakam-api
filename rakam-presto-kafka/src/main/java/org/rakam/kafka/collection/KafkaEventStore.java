package org.rakam.kafka.collection;

import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.airlift.log.Logger;
import kafka.common.FailedToSendMessageException;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.SourceFilteredRecordWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.rakam.collection.Event;
import org.rakam.collection.SchemaField;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.plugin.EventStore;
import org.rakam.util.KByteArrayOutputStream;

import javax.inject.Inject;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

@Singleton
public class KafkaEventStore implements EventStore, LeaderSelectorListener {
    final static Logger LOGGER = Logger.get(KafkaEventStore.class);
    final static String ZK_OFFSET_PATH = "/collectionOffsets";

    private final Producer<byte[], byte[]> producer;
    private final Set<String> sourceFields;
    ScheduledExecutorService executorService;

    ThreadLocal<KByteArrayOutputStream> buffer = new ThreadLocal<KByteArrayOutputStream>() {
        @Override
        protected KByteArrayOutputStream initialValue() {
            return new KByteArrayOutputStream(50000);
        }
    };

    @Inject
    public KafkaEventStore(@Named("event.store.kafka") KafkaConfig config, FieldDependencyBuilder.FieldDependency fieldDependency) {
        config = checkNotNull(config, "config is null");
        this.sourceFields = Sets.union(fieldDependency.dependentFields.keySet(),
                fieldDependency.constantFields.stream().map(SchemaField::getName)
                        .collect(Collectors.toSet()));

        Properties props = new Properties();
        props.put("metadata.broker.list", config.getNodes().stream().map(HostAndPort::toString).collect(Collectors.joining(",")));
        props.put("serializer.class", config.SERIALIZER);

        ProducerConfig producerConfig = new ProducerConfig(props);
        this.producer = new Producer(producerConfig);

        CuratorFramework client = CuratorFrameworkFactory.newClient(config.getZookeeperNode().toString(),
                new ExponentialBackoffRetry(1000, 3));
        client.start();

        try {
            if(client.checkExists().forPath(ZK_OFFSET_PATH) == null)
                client.create().forPath(ZK_OFFSET_PATH);
        } catch (Exception e) {
            LOGGER.error(e, format("Couldn't create event offset path %s", ZK_OFFSET_PATH));
        }

        new LeaderSelector(client, ZK_OFFSET_PATH, this).start();
    }

    @Override
    public void store(Event event) {
        GenericDatumWriter writer = new SourceFilteredRecordWriter(event.properties().getSchema(), GenericData.get(), sourceFields);
        KByteArrayOutputStream out = buffer.get();

        int startPosition = out.position();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);

        try {
            writer.write(event.properties(), encoder);
        } catch (Exception e) {
            throw new RuntimeException("Couldn't serialize event", e);
        }

        int endPosition = out.position();
        // TODO: find a way to make it zero-copy
        byte[] copy = out.copy(startPosition, endPosition);

        if(out.remaining() < 1000) {
            out.position(0);
        }
        try {
            producer.send(new KeyedMessage<>(event.project()+"_"+event.collection(), copy));
        } catch (FailedToSendMessageException e) {
            throw new RuntimeException("Couldn't send event to Kafka", e);
        }
    }

    @Override
    public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
        if(executorService == null) {
            ThreadFactory build = new ThreadFactoryBuilder()
                    .setNameFormat("kafka-offset-worker").build();
            executorService = Executors.newSingleThreadScheduledExecutor(build);
        }
//        executorService.scheduleAtFixedRate(kafkaManager::updateOffsets, updateInterval, updateInterval, TimeUnit.SECONDS);
    }

    @Override
    public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
        if(!connectionState.isConnected()) {
            if(executorService != null) {
                executorService.shutdown();
                executorService = null;
            }
        }
    }
}
