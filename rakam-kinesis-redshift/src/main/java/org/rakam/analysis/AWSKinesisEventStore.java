package org.rakam.analysis;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.google.inject.Inject;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.rakam.collection.Event;
import org.rakam.plugin.EventStore;
import org.rakam.util.KByteArrayOutputStream;

import static org.rakam.analysis.util.SerializationHelper.encodeInt;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/07/15 06:47.
 */
public class AWSKinesisEventStore implements EventStore {
    private final AmazonKinesisClient kinesis;
    private final AWSConfig config;

    ThreadLocal<KByteArrayOutputStream> buffer = new ThreadLocal<KByteArrayOutputStream>() {
        @Override
        protected KByteArrayOutputStream initialValue() {
            return new KByteArrayOutputStream(50000);
        }
    };

    @Inject
    public AWSKinesisEventStore(AWSConfig config) {
        AWSCredentials credentials = new BasicAWSCredentials(config.getAccessKey(), config.getSecretAccessKey());
        this.kinesis = new AmazonKinesisClient(credentials);
        this.config = config;
    }

    @Override
    public void store(Event event) {
        DatumWriter writer = new GenericDatumWriter(event.properties().getSchema());
        KByteArrayOutputStream out = buffer.get();

        int startPosition = out.position();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);

        byte[] project = event.project().getBytes();
        byte[] collection = event.collection().getBytes();

        try {
            encodeInt(project.length, out);
            out.write(project);
            encodeInt(collection.length, out);
            out.write(collection);

            writer.write(event.properties(), encoder);
        } catch (Exception e) {
            throw new RuntimeException("Couldn't serialize event", e);
        }

        int endPosition = out.position();
        // TODO: find a way to make it zero-copy

        if(out.remaining() < 1000) {
            out.position(0);
        }

        try {
            kinesis.putRecord(config.getKinesisStream(), out.getBuffer(startPosition, endPosition - startPosition),
                    Integer.toString(((int) Math.random()*100)));
        } catch (ResourceNotFoundException e) {
            throw new RuntimeException("Couldn't send event to Amazon Kinesis", e);
        }
    }
}