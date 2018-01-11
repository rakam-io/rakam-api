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
import org.rakam.util.RakamException;

import javax.inject.Inject;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.netty.buffer.PooledByteBufAllocator.DEFAULT;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;

public class AWSKinesisEventStore
        implements EventStore {
    private final static Logger LOGGER = Logger.get(AWSKinesisEventStore.class);

    private final AmazonKinesisAsyncClient kinesis;
    private final AWSConfig config;
    private final S3BulkEventStore bulkClient;
    private final KinesisProducer producer;

    @Inject
    public AWSKinesisEventStore(AWSConfig config,
                                Metastore metastore,
                                FieldDependency fieldDependency) {
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
                producerConfiguration.setKinesisEndpoint(url.getHost());
                producerConfiguration.setKinesisPort(url.getPort());
                producerConfiguration.setVerifyCertificate(false);
            } catch (MalformedURLException e) {
                throw new IllegalStateException(String.format("Kinesis endpoint is invalid: %s", config.getKinesisEndpoint()));
            }
        }
        producer = new KinesisProducer(producerConfiguration);
    }

    public CompletableFuture<int[]> storeBatchInline(List<Event> events) {
        ByteBuf[] byteBufs = new ByteBuf[events.size()];

        try {
            for (int i = 0; i < events.size(); i++) {
                Event event = events.get(i);
                ByteBuf buffer = getBuffer(event);
                ByteBuffer data = buffer.nioBuffer();
                try {
                    producer.addUserRecord(config.getEventStoreStreamName(),
                            getPartitionKey(event),
                            data);
                } catch (IllegalArgumentException e) {
                    if (data.remaining() > 1048576) {
                        throw new RakamException("Too many event properties, the total size of an event must be less than or equal to 1MB, got " + data.remaining(),
                                BAD_REQUEST);
                    }
                }
                byteBufs[i] = buffer;
            }

            // TODO: async callback?
            producer.flush();
        } finally {
            for (ByteBuf byteBuf : byteBufs) {
                if (byteBuf != null) {
                    byteBuf.release();
                }
            }
        }

        return EventStore.COMPLETED_FUTURE_BATCH;
    }

    @Override
    public void storeBulk(List<Event> events) {
        if (events.isEmpty()) {
            return;
        }
        String project = events.get(0).project();
        try {
            bulkClient.upload(project, events, 3);
        } catch (OutOfMemoryError e) {
            LOGGER.error(e, "OOM error while uploading bulk");
            throw new RakamException("Too much data", HttpResponseStatus.BAD_REQUEST);
        } catch (Throwable e) {
            LOGGER.error(e);
            throw new RakamException("An error occurred while storing events", INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public CompletableFuture<int[]> storeBatchAsync(List<Event> events) {
        return storeBatchInline(events);
    }

    @Override
    public CompletableFuture<Void> storeAsync(Event event) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        store(event, future, 3);
        return future;
    }

    private String getPartitionKey(Event event) {
        Object user = event.getAttribute("_user");
        return event.project() + "|" + (user == null ? event.collection() : user.toString());
    }

    public void store(Event event, CompletableFuture<Void> future, int tryCount) {
        store(getBuffer(event), getPartitionKey(event), future, tryCount);
    }

    public void store(ByteBuf buffer, String partitionKey, CompletableFuture<Void> future, int tryCount) {
        try {
            kinesis.putRecordAsync(config.getEventStoreStreamName(), buffer.nioBuffer(), partitionKey, new AsyncHandler<PutRecordRequest, PutRecordResult>() {
                @Override
                public void onError(Exception e) {
                    try {
                        if (e instanceof ResourceNotFoundException) {
                            try {
                                KinesisUtils.createAndWaitForStreamToBecomeAvailable(kinesis, config.getEventStoreStreamName(), 1);
                            } catch (Exception e1) {
                                throw new RuntimeException("Couldn't send event to Amazon Kinesis", e);
                            }
                        }

                        LOGGER.error(e);
                        if (tryCount > 0) {
                            store(buffer, partitionKey, future, tryCount - 1);
                        } else {
                            buffer.release();
                            future.completeExceptionally(new RakamException(INTERNAL_SERVER_ERROR));
                        }
                    } catch (Throwable e1) {
                        buffer.release();
                        LOGGER.error(e1);
                    }
                }

                @Override
                public void onSuccess(PutRecordRequest request, PutRecordResult putRecordResult) {
                    try {
                        buffer.release();
                    } finally {
                        future.complete(null);
                    }
                }
            });
        } catch (Exception e) {
            buffer.release();
        }
    }

    private ByteBuf getBuffer(Event event) {
        DatumWriter writer = new FilteredRecordWriter(event.properties().getSchema(), GenericData.get());
        ByteBuf buffer = DEFAULT.buffer(100);
        buffer.writeByte(2);

        BinaryEncoder encoder = EncoderFactory.get()
                .directBinaryEncoder(new ByteBufOutputStream(buffer), null);

        try {
            encoder.writeString(event.collection());

            writer.write(event.properties(), encoder);
        } catch (Exception e) {
            throw new RuntimeException("Couldn't serialize event", e);
        }

        return buffer;
    }
}