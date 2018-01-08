package org.rakam.clickhouse.collection;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.google.common.base.Throwables;
import com.google.common.io.LittleEndianDataOutputStream;
import io.airlift.log.Logger;
import org.apache.avro.generic.GenericRecord;
import org.rakam.aws.AWSConfig;
import org.rakam.aws.kinesis.KinesisUtils;
import org.rakam.clickhouse.ClickHouseConfig;
import org.rakam.collection.Event;
import org.rakam.config.ProjectConfig;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.SyncEventStore;

import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.rakam.clickhouse.collection.ClickHouseEventStore.writeValue;
import static org.rakam.clickhouse.collection.ClickHouseEventStore.writeVarInt;
import static org.rakam.collection.FieldType.DATE;

public class AWSKinesisClickhouseEventStore
        implements SyncEventStore {
    private final static Logger LOGGER = Logger.get(AWSKinesisClickhouseEventStore.class);
    private static final int BATCH_SIZE = 500;
    private final AmazonKinesisClient kinesis;
    private final AWSConfig config;
    private final ClickHouseEventStore bulkClient;
    private final ProjectConfig projectConfig;
//    private final KinesisProducer producer;

    @Inject
    public AWSKinesisClickhouseEventStore(AWSConfig config, ProjectConfig projectConfig, ClickHouseConfig clickHouseConfig) {
        kinesis = new AmazonKinesisClient(config.getCredentials());
        kinesis.setRegion(config.getAWSRegion());
        if (config.getKinesisEndpoint() != null) {
            kinesis.setEndpoint(config.getKinesisEndpoint());
        }
        this.config = config;
        this.projectConfig = projectConfig;
        this.bulkClient = new ClickHouseEventStore(projectConfig, clickHouseConfig);

        KinesisProducerConfiguration producerConfiguration = new KinesisProducerConfiguration()
                .setRegion(config.getRegion())
                .setCredentialsProvider(config.getCredentials());
//        producer = new KinesisProducer(producerConfiguration);
    }

    public int[] storeBatchInline(List<Event> events, int offset, int limit) {
        PutRecordsRequestEntry[] records = new PutRecordsRequestEntry[limit];

        for (int i = 0; i < limit; i++) {
            Event event = events.get(offset + i);
            PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry()
                    .withData(getBuffer(event))
                    .withPartitionKey(event.project() + "|" + event.collection());
            records[i] = putRecordsRequestEntry;
        }

        try {
            PutRecordsResult putRecordsResult = kinesis.putRecords(new PutRecordsRequest()
                    .withRecords(records)
                    .withStreamName(config.getEventStoreStreamName()));
            if (putRecordsResult.getFailedRecordCount() > 0) {
                int[] failedRecordIndexes = new int[putRecordsResult.getFailedRecordCount()];
                int idx = 0;

                Map<String, Integer> errors = new HashMap<>();

                List<PutRecordsResultEntry> recordsResponse = putRecordsResult.getRecords();
                for (int i = 0; i < recordsResponse.size(); i++) {
                    if (recordsResponse.get(i).getErrorCode() != null) {
                        failedRecordIndexes[idx++] = i;
                        errors.compute(recordsResponse.get(i).getErrorMessage(), (k, v) -> v == null ? 1 : v++);
                    }
                }

                LOGGER.warn("Error in Kinesis putRecords: %d records.", putRecordsResult.getFailedRecordCount(), errors.toString());
                return failedRecordIndexes;
            } else {
                return EventStore.SUCCESSFUL_BATCH;
            }
        } catch (ResourceNotFoundException e) {
            try {
                KinesisUtils.createAndWaitForStreamToBecomeAvailable(kinesis, config.getEventStoreStreamName(), 1);
                return storeBatchInline(events, offset, limit);
            } catch (Exception e1) {
                throw new RuntimeException("Couldn't send event to Amazon Kinesis", e);
            }
        }
    }

    @Override
    public void storeBulk(List<Event> events) {
        if (events.isEmpty()) {
            return;
        }
        bulkClient.storeBatch(events);
    }

    @Override
    public void store(Event event) {
        try {
            kinesis.putRecord(config.getEventStoreStreamName(),
                    getBuffer(event), event.project() + "|" + event.collection());
        } catch (ResourceNotFoundException e) {
            try {
                KinesisUtils.createAndWaitForStreamToBecomeAvailable(kinesis, config.getEventStoreStreamName(), 1);
            } catch (Exception e1) {
                throw new RuntimeException("Couldn't send event to Amazon Kinesis", e);
            }
        }
    }

    @Override
    public int[] storeBatch(List<Event> events) {
        if (events.size() > BATCH_SIZE) {
            ArrayList<Integer> errors = null;
            int cursor = 0;

            while (cursor < events.size()) {
                int loopSize = Math.min(BATCH_SIZE, events.size() - cursor);

                int[] errorIndexes = storeBatchInline(events, cursor, loopSize);
                if (errorIndexes.length > 0) {
                    if (errors == null) {
                        errors = new ArrayList<>(errorIndexes.length);
                    }

                    for (int errorIndex : errorIndexes) {
                        errors.add(errorIndex + cursor);
                    }
                }
                cursor += loopSize;
            }

            return errors == null ? EventStore.SUCCESSFUL_BATCH : errors.stream().mapToInt(Integer::intValue).toArray();
        } else {
            return storeBatchInline(events, 0, events.size());
        }
    }

    private ByteBuffer getBuffer(Event event) {
        SharedByteArrayOutputStream buffer = new SharedByteArrayOutputStream(event.properties().getSchema().getFields().size() * 8);
        LittleEndianDataOutputStream out = new LittleEndianDataOutputStream(buffer);

        GenericRecord record = event.properties();
        Object time = record.get(projectConfig.getTimeColumn());
        try {
            int size = event.schema().size();
            writeVarInt(size, out);
            writeValue(time == null ? 0 : ((int) (((long) time) / 86400000)), DATE, out);

            for (int i = 0; i < size; i++) {
                writeValue(record.get(i), event.schema().get(i).getType(), out);
            }
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }

        return buffer.toByteBuffer();
    }

    public static class SharedByteArrayOutputStream extends ByteArrayOutputStream {
        public SharedByteArrayOutputStream(int estimatedSize) {
            super(estimatedSize);
        }

        public ByteBuffer toByteBuffer() {
            return ByteBuffer.wrap(buf, 0, count);
        }
    }
}