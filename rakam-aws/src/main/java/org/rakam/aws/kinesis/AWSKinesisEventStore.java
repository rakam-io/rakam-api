package org.rakam.aws.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import io.airlift.log.Logger;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.avro.generic.FilteredRecordWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.aws.AWSConfig;
import org.rakam.aws.s3.S3BulkEventStore;
import org.rakam.collection.Event;
import org.rakam.collection.FieldDependencyBuilder.FieldDependency;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.SyncEventStore;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryResult;
import org.rakam.util.KByteArrayOutputStream;
import org.rakam.util.RakamException;

import javax.inject.Inject;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AWSKinesisEventStore
        implements SyncEventStore
{
    private final static Logger LOGGER = Logger.get(AWSKinesisEventStore.class);

    private final AmazonKinesisClient kinesis;
    private final AWSConfig config;
    private static final int BATCH_SIZE = 500;
    private final S3BulkEventStore bulkClient;
    private final KinesisProducer producer;

    private ThreadLocal<KByteArrayOutputStream> buffer = new ThreadLocal<KByteArrayOutputStream>()
    {
        @Override
        protected KByteArrayOutputStream initialValue()
        {
            return new KByteArrayOutputStream(1000000);
        }
    };

    @Inject
    public AWSKinesisEventStore(AWSConfig config,
            Metastore metastore,
            FieldDependency fieldDependency)
    {
        kinesis = new AmazonKinesisClient(config.getCredentials());
        kinesis.setRegion(config.getAWSRegion());
        if (config.getKinesisEndpoint() != null) {
            kinesis.setEndpoint(config.getKinesisEndpoint());
        }
        this.config = config;
        this.bulkClient = new S3BulkEventStore(metastore, config, fieldDependency);

        KinesisProducerConfiguration producerConfiguration = new KinesisProducerConfiguration()
                .setRegion(config.getRegion())
                .setCredentialsProvider(config.getCredentials());
        if (config.getKinesisEndpoint() != null) {
            producerConfiguration.setCustomEndpoint(config.getKinesisEndpoint());
        }
        producer = new KinesisProducer(producerConfiguration);
    }

    public void storeBatchInline(List<Event> events)
    {
        for (Event event : events) {
            producer.addUserRecord(config.getEventStoreStreamName(),
                    event.project() + "|" + event.collection(), getBuffer(event));
        }

        producer.flushSync();
    }

    @Override
    public void storeBulk(List<Event> events)
    {
        if (events.isEmpty()) {
            return;
        }
        String project = events.get(0).project();
        try {
            bulkClient.upload(project, events);
        }
        catch (OutOfMemoryError e) {
            LOGGER.error(e, "OOM error while uploading bulk");
            throw new RakamException("Too much data", HttpResponseStatus.BAD_REQUEST);
        }
        catch (Throwable e) {
            LOGGER.error(e);
            throw new RakamException("An error occurred while storing events", HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public QueryExecution commit(String project, String collection)
    {
        return QueryExecution.completedQueryExecution(null, QueryResult.empty());
    }

    @Override
    public int[] storeBatch(List<Event> events)
    {
        storeBatchInline(events);
        return EventStore.SUCCESSFUL_BATCH;
    }

    @Override
    public void store(Event event)
    {
        try {
            kinesis.putRecord(config.getEventStoreStreamName(), getBuffer(event),
                    event.project() + "|" + event.collection());
        }
        catch (ResourceNotFoundException e) {
            try {
                KinesisUtils.createAndWaitForStreamToBecomeAvailable(kinesis, config.getEventStoreStreamName(), 1);
            }
            catch (Exception e1) {
                throw new RuntimeException("Couldn't send event to Amazon Kinesis", e);
            }
        }
    }

    private ByteBuffer getBuffer(Event event)
    {
        DatumWriter writer = new FilteredRecordWriter(event.properties().getSchema(), GenericData.get());
        KByteArrayOutputStream out = buffer.get();

        int startPosition = out.position();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
        out.write(0);

        try {
            writer.write(event.properties(), encoder);
        }
        catch (Exception e) {
            throw new RuntimeException("Couldn't serialize event", e);
        }

        int endPosition = out.position();
        // TODO: find a way to make it zero-copy

        if (out.remaining() < 1000) {
            out.position(0);
        }

        return out.getBuffer(startPosition, endPosition - startPosition);
    }
}