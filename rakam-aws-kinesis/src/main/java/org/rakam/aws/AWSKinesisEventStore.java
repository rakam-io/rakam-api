package org.rakam.aws;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.RecordGenericRecordWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.rakam.collection.Event;
import org.rakam.collection.event.FieldDependencyBuilder;
import org.rakam.plugin.EventStore;
import org.rakam.util.KByteArrayOutputStream;

import javax.inject.Inject;
import java.util.Set;

import static org.rakam.aws.KinesisUtils.createAndWaitForStreamToBecomeAvailable;

public class AWSKinesisEventStore implements EventStore {
    private final AmazonKinesisClient kinesis;
    private final AWSConfig config;
    private final Set<String> sourceFields;

    ThreadLocal<KByteArrayOutputStream> buffer = new ThreadLocal<KByteArrayOutputStream>() {
        @Override
        protected KByteArrayOutputStream initialValue() {
            return new KByteArrayOutputStream(50000);
        }
    };

    @Inject
    public AWSKinesisEventStore(AWSConfig config, FieldDependencyBuilder.FieldDependency dependency) {
        this.kinesis = new AmazonKinesisClient(config.getCredentials());
        this.sourceFields = dependency.dependentFields.keySet();
        this.config = config;
    }

    @Override
    public void store(Event event) {
        DatumWriter writer = new RecordGenericRecordWriter(event.properties().getSchema(), GenericData.get(), sourceFields);
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

        if(out.remaining() < 1000) {
            out.position(0);
        }

        try {
            kinesis.putRecord(config.getEventStoreStreamName(), out.getBuffer(startPosition, endPosition - startPosition),
                    event.project()+"_"+event.collection());
        } catch (ResourceNotFoundException e) {
            try {
                createAndWaitForStreamToBecomeAvailable(kinesis, config.getEventStoreStreamName(), 1);
            } catch (Exception e1) {
                throw new RuntimeException("Couldn't send event to Amazon Kinesis", e);
            }
        }
    }
}