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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by buremba <Burak Emre Kabakcı> on 14/02/15 03:25.
 */
public class KafkaEventStore implements EventStore {
    final static Logger LOGGER = LoggerFactory.getLogger(KafkaEventStore.class);

    private final Producer<byte[], byte[]> producer;
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(new ByteArrayOutputStream(), null);

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
        ByteArrayOutputStream out = new ByteArrayOutputStream(64);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, this.encoder);

        try {
            writer.write(event.properties(), encoder);
        } catch (IOException e) {
            throw new RuntimeException("Couldn't serialize event", e);
        } finally {
            try {
                encoder.flush();
            } catch (IOException e) {
                throw new RuntimeException("Couldn't flush the buffer", e);
            }
        }

        try {
            producer.send(new KeyedMessage<>(event.project()+"_"+event.collection(), out.toByteArray()));
        } catch (FailedToSendMessageException e) {
            throw new RuntimeException("Couldn't send event to Kafka", e);
        }
    }
}
