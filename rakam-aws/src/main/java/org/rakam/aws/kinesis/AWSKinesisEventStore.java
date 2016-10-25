package org.rakam.aws.kinesis;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import io.airlift.log.Logger;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
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
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryResult;
import org.rakam.util.RakamException;

import javax.inject.Inject;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.netty.buffer.PooledByteBufAllocator.DEFAULT;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.valueOf;

public class AWSKinesisEventStore
        implements EventStore
{
    private final static Logger LOGGER = Logger.get(AWSKinesisEventStore.class);

    private final AmazonKinesisAsyncClient kinesis;
    private final AWSConfig config;
    private final S3BulkEventStore bulkClient;
    private final KinesisProducer producer;

    @Inject
    public AWSKinesisEventStore(AWSConfig config,
            Metastore metastore,
            FieldDependency fieldDependency)
    {
        kinesis = new AmazonKinesisAsyncClient(config.getCredentials());
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

    public CompletableFuture<int[]> storeBatchInline(List<Event> events)
    {
        ByteBuf[] byteBufs = new ByteBuf[events.size()];

        try {
            for (int i = 0; i < events.size(); i++) {
                Event event = events.get(i);
                ByteBuf buffer = getBuffer(event);
//                Object user = event.getAttribute("_user");
                producer.addUserRecord(config.getEventStoreStreamName(),
                        event.project() + "|" + event.collection(),
//                        (event.project() + "|" + user == null ? event.collection() : user.toString()),
                        buffer.nioBuffer());
                byteBufs[i] = buffer;
            }

            // TODO: callback?
            producer.flush();
        }
        finally {
            for (ByteBuf byteBuf : byteBufs) {
                if (byteBuf != null) {
                    byteBuf.release();
                }
            }
        }

        return EventStore.COMPLETED_FUTURE_BATCH;
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
            throw new RakamException("An error occurred while storing events", INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public QueryExecution commit(String project, String collection)
    {
        return QueryExecution.completedQueryExecution(null, QueryResult.empty());
    }

    @Override
    public CompletableFuture<int[]> storeBatchAsync(List<Event> events)
    {
        return storeBatchInline(events);
    }

    @Override
    public CompletableFuture<Void> storeAsync(Event event)
    {
        CompletableFuture<Void> future = new CompletableFuture<>();
        store(event, future, 3);
        return future;
    }

    public void store(Event event, CompletableFuture<Void> future, int tryCount)
    {
        ByteBuf buffer = getBuffer(event);
        kinesis.putRecordAsync(config.getEventStoreStreamName(), buffer.nioBuffer(),
                event.project() + "|" + event.collection(), new AsyncHandler<PutRecordRequest, PutRecordResult>()
                {
                    @Override
                    public void onError(Exception e)
                    {
                        if (e instanceof ResourceNotFoundException) {
                            try {
                                KinesisUtils.createAndWaitForStreamToBecomeAvailable(kinesis, config.getEventStoreStreamName(), 1);
                            }
                            catch (Exception e1) {
                                throw new RuntimeException("Couldn't send event to Amazon Kinesis", e);
                            }
                        }

                        LOGGER.error(e);
                        if (tryCount > 0) {
                            store(event, future, tryCount - 1);
                        }
                        else {
                            buffer.release();
                            future.completeExceptionally(new RakamException(INTERNAL_SERVER_ERROR));
                        }
                    }

                    @Override
                    public void onSuccess(PutRecordRequest request, PutRecordResult putRecordResult)
                    {
                        buffer.release();
                        future.complete(null);
                    }
                });
    }

    private ByteBuf getBuffer(Event event)
    {
        DatumWriter writer = new FilteredRecordWriter(event.properties().getSchema(), GenericData.get());
        ByteBuf buffer = DEFAULT.buffer(100);
        buffer.writeByte(0);

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
}