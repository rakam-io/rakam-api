package org.rakam.collection.event.datastore;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import kafka.common.FailedToSendMessageException;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.rakam.collection.event.EventStore;
import org.rakam.config.KafkaConfig;
import org.rakam.model.Event;
import org.rakam.util.HostAddress;
import org.rakam.util.KByteArrayOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by buremba <Burak Emre Kabakcı> on 14/02/15 03:25.
 */
public class KafkaEventStore implements EventStore {
    final static Logger LOGGER = LoggerFactory.getLogger(KafkaEventStore.class);

    private final Producer<byte[], byte[]> producer;
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(new ByteArrayOutputStream(), null);

    ThreadLocal<KByteArrayOutputStream> buffer = new ThreadLocal<KByteArrayOutputStream>() {
        @Override
        protected KByteArrayOutputStream initialValue() {
            return new KByteArrayOutputStream(50000);
        }
    };

    @Inject
    public KafkaEventStore(@Named("event.store.kafka") KafkaConfig config) {
        config = checkNotNull(config, "config is null");

        Properties props = new Properties();
        props.put("metadata.broker.list", config.getNodes().stream().map(HostAddress::toString).collect(Collectors.joining(",")));
        props.put("serializer.class", config.SERIALIZER);

        ProducerConfig producerConfig = new ProducerConfig(props);
        this.producer = new Producer(producerConfig);
    }

    @Override
    public void store(Event event) {
        // TODO: find a way to make it zero-copy
        DatumWriter writer = new GenericDatumWriter(event.properties().getSchema());
        KByteArrayOutputStream out = buffer.get();

        int startPosition = out.position();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);

        try {
            writer.write(event.properties(), encoder);
        } catch (Exception e) {
            throw new RuntimeException("Couldn't serialize event", e);
        }

        int endPosition = out.position();
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
}
