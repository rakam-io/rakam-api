package org.rakam.clickhouse.collection;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.inject.name.Named;
import io.airlift.log.Logger;
import io.netty.util.CharsetUtil;
import org.apache.avro.generic.FilteredRecordWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.aws.AWSConfig;
import org.rakam.aws.kinesis.KinesisUtils;
import org.rakam.clickhouse.ClickHouseConfig;
import org.rakam.clickhouse.ClickHouseQueryExecutor;
import org.rakam.collection.Event;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.SyncEventStore;
import org.rakam.report.ChainQueryExecution;
import org.rakam.report.DelegateQueryExecution;
import org.rakam.report.QueryError;
import org.rakam.report.QueryExecution;
import org.rakam.util.JsonHelper;
import org.rakam.util.KByteArrayOutputStream;
import org.rakam.util.QueryFormatter;
import org.rakam.util.RakamException;
import org.rakam.util.StandardErrors;
import org.rakam.util.ValidationUtil;

import javax.inject.Inject;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static java.lang.String.format;
import static org.rakam.clickhouse.collection.ClickHouseEventStore.writeValue;
import static org.rakam.collection.FieldType.DATE;

public class AWSKinesisClickhouseEventStore
        implements SyncEventStore
{
    private final static Logger LOGGER = Logger.get(AWSKinesisClickhouseEventStore.class);

    private final AmazonKinesisClient kinesis;
    private final AWSConfig config;
    private static final int BATCH_SIZE = 500;
    private final ClickHouseEventStore bulkClient;

    private ThreadLocal<KByteArrayOutputStream> buffer = new ThreadLocal<KByteArrayOutputStream>()
    {
        @Override
        protected KByteArrayOutputStream initialValue()
        {
            return new KByteArrayOutputStream(1000000);
        }
    };

    @Inject
    public AWSKinesisClickhouseEventStore(AWSConfig config,
            ClickHouseQueryExecutor executor,
            ClickHouseConfig clickHouseConfig)
    {
        kinesis = new AmazonKinesisClient(config.getCredentials());
        kinesis.setRegion(config.getAWSRegion());
        if (config.getKinesisEndpoint() != null) {
            kinesis.setEndpoint(config.getKinesisEndpoint());
        }
        this.config = config;
        this.bulkClient = new ClickHouseEventStore(clickHouseConfig);

        KinesisProducerConfiguration producerConfiguration = new KinesisProducerConfiguration()
                .setRegion(config.getRegion())
                .setCredentialsProvider(config.getCredentials());
//        KinesisProducer producer = new KinesisProducer(producerConfiguration);
    }

    public int[] storeBatchInline(List<Event> events, int offset, int limit)
    {
        PutRecordsRequestEntry[] records = new PutRecordsRequestEntry[limit];

        for (int i = 0; i < limit; i++) {
            Event event = events.get(offset + i);
            PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry()
                    .withData(getBuffer(event, false))
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
            }
            else {
                return EventStore.SUCCESSFUL_BATCH;
            }
        }
        catch (ResourceNotFoundException e) {
            try {
                KinesisUtils.createAndWaitForStreamToBecomeAvailable(kinesis, config.getEventStoreStreamName(), 1);
                return storeBatchInline(events, offset, limit);
            }
            catch (Exception e1) {
                throw new RuntimeException("Couldn't send event to Amazon Kinesis", e);
            }
        }
    }

    @Override
    public void storeBulk(List<Event> events)
    {
        bulkClient.storeBatch(events);
    }

    @Override
    public int[] storeBatch(List<Event> events)
    {
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
        }
        else {
            return storeBatchInline(events, 0, events.size());
        }
    }

    @Override
    public void store(Event event)
    {
        try {
            kinesis.putRecord(config.getEventStoreStreamName(), getBuffer(event, false),
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

    private ByteBuffer getBuffer(Event event, boolean flushed)
    {
        KByteArrayOutputStream buffer = this.buffer.get();
        int position = buffer.position();
        DataOutputStream out = new DataOutputStream(buffer);

        GenericRecord record = event.properties();
        Object time = record.get("_time");
        try {
            writeValue(time == null ? 0 : ((int) (((long) time) / 86400)), DATE, out);

            for (int i = 0; i < event.schema().size(); i++) {
                writeValue(record.get(i), event.schema().get(i).getType(), out);
            }
        }
        catch (IOException e) {
            if (flushed) {
                throw Throwables.propagate(e);
            }

            buffer.position(0);
            return getBuffer(event, true);
        }

        if (buffer.remaining() < 1000) {
            buffer.position(0);
        }

        return buffer.getBuffer(position, out.size());
    }
}