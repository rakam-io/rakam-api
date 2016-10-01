package org.rakam.aws.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import io.airlift.log.Logger;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.avro.generic.FilteredRecordWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.ByteBufferOutputStream;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.aws.AWSConfig;
import org.rakam.aws.s3.S3BulkEventStore;
import org.rakam.collection.Event;
import org.rakam.collection.FieldDependencyBuilder.FieldDependency;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.SyncEventStore;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryResult;
import org.rakam.util.RakamException;

import javax.inject.Inject;

import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static io.netty.buffer.PooledByteBufAllocator.DEFAULT;

public class AWSKinesisEventStore
        implements SyncEventStore
{
    private final static Logger LOGGER = Logger.get(AWSKinesisEventStore.class);

    private final AmazonKinesisClient kinesis;
    private final AWSConfig config;
    private final S3BulkEventStore bulkClient;
    private final KinesisProducer producer;

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
            try {
                URL url = new URL(config.getKinesisEndpoint());
                producerConfiguration.setCustomEndpoint(url.getHost());
                producerConfiguration.setPort(url.getPort());
                producerConfiguration.setVerifyCertificate(false);
            }
            catch (MalformedURLException e) {
                throw new IllegalStateException(String.format("Kinesis endpoint is invalid: %s", config.getKinesisEndpoint()));
            }
        }
        producer = new KinesisProducer(producerConfiguration);
    }

    public void storeBatchInline(List<Event> events)
    {
        ByteBuf[] byteBufs = new ByteBuf[events.size()];

        try {
            for (int i = 0; i < events.size(); i++) {
                Event event = events.get(i);
                ByteBuf buffer = getBuffer(event);
                producer.addUserRecord(config.getEventStoreStreamName(),
                        event.project() + "|" + event.collection(), buffer.nioBuffer());
                byteBufs[i] = buffer;
            }

            producer.flushSync();
        }
        finally {
            for (ByteBuf byteBuf : byteBufs) {
                if (byteBuf != null) {
                    byteBuf.release();
                }
            }
        }
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
        ByteBuf buffer = getBuffer(event);
        try {
            kinesis.putRecord(config.getEventStoreStreamName(), buffer.nioBuffer(),
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
        finally {
            buffer.release();
        }
    }

    private ByteBuf getBuffer(Event event)
    {
        DatumWriter writer = new FilteredRecordWriter(event.properties().getSchema(), GenericData.get());
        ByteBuf buffer = DEFAULT.buffer(100);

        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(
                new ByteBufOutputStream(buffer), null);

        try {
            writer.write(event.properties(), encoder);
        }
        catch (Exception e) {
            throw new RuntimeException("Couldn't serialize event", e);
        }

        return buffer;
    }

    public static class ByteBufferOutputStream
            extends OutputStream
    {
        private final ByteBuffer buffer;

        public ByteBufferOutputStream(ByteBuffer buffer)
        {
            this.buffer = buffer;
        }

        @Override
        public void write(int b)
                throws IOException
        {
            buffer.put((byte) b);
        }

        @Override
        public void write(byte[] b)
                throws IOException
        {
            buffer.put(b);
        }

        @Override
        public void write(byte[] b, int off, int len)
                throws IOException
        {
            buffer.put(b, off, len);
        }

        @Override
        public void flush()
                throws IOException
        {
            super.flush();
        }

        @Override
        public void close()
                throws IOException
        {
            super.close();
        }
    }
}