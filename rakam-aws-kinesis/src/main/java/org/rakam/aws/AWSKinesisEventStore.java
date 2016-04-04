package org.rakam.aws;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.google.common.base.Throwables;
import com.google.inject.name.Named;
import io.airlift.log.Logger;
import org.apache.avro.generic.FilteredRecordWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.analysis.metadata.QueryMetadataStore;
import org.rakam.collection.Event;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.EventStore;
import org.rakam.presto.analysis.PrestoConfig;
import org.rakam.presto.analysis.PrestoQueryExecutor;
import org.rakam.util.KByteArrayOutputStream;
import org.rakam.util.QueryFormatter;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.rakam.aws.KinesisUtils.createAndWaitForStreamToBecomeAvailable;

public class AWSKinesisEventStore implements EventStore {
    private final static Logger LOGGER = Logger.get(AWSKinesisEventStore.class);

    private final AmazonKinesisClient kinesis;
    private final AWSConfig config;
    private static final int BATCH_SIZE = 500;
    private final S3BulkEventStore bulkClient;
    private final PrestoQueryExecutor executor;
    private final PrestoConfig prestoConfig;
    private final JDBCPoolDataSource dataSource;
    private final QueryMetadataStore queryMetadataStore;

    ThreadLocal<KByteArrayOutputStream> buffer = new ThreadLocal<KByteArrayOutputStream>() {
        @Override
        protected KByteArrayOutputStream initialValue() {
            return new KByteArrayOutputStream(1000000);
        }
    };

    @Inject
    public AWSKinesisEventStore(AWSConfig config,
                                Metastore metastore,
                                PrestoQueryExecutor executor,
                                QueryMetadataStore queryMetadataStore,
                                @Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource,
                                PrestoConfig prestoConfig,
                                FieldDependencyBuilder.FieldDependency fieldDependency) {
        kinesis = new AmazonKinesisClient(config.getCredentials());
        kinesis.setRegion(config.getAWSRegion());
        if (config.getKinesisEndpoint() != null) {
            kinesis.setEndpoint(config.getKinesisEndpoint());
        }
        this.config = config;
        this.executor = executor;
        this.dataSource = dataSource;
        this.prestoConfig = prestoConfig;
        this.queryMetadataStore = queryMetadataStore;
        this.bulkClient = new S3BulkEventStore(metastore, config, fieldDependency);
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
                createAndWaitForStreamToBecomeAvailable(kinesis, config.getEventStoreStreamName(), 1);
                return storeBatchInline(events, offset, limit);
            } catch (Exception e1) {
                throw new RuntimeException("Couldn't send event to Amazon Kinesis", e);
            }
        }
    }

    @Override
    public void storeBulk(List<Event> events, boolean commit) {
        String project = events.get(0).project();
        bulkClient.upload(project, events);
        if (commit) {
            events.stream().map(e -> e.collection()).distinct().parallel()
                    .forEach(collection -> commit(project, collection));
        }
    }

    @Override
    public void commit(String project, String collection) {
        try (Connection conn = dataSource.getConnection()) {
            Statement statement = conn.createStatement();
            statement.execute(format("SELECT * FROM middleware_transaction WHERE project = ? FOR UPDATE", project, collection));

            String middlewareTable = format("%s.%s.%s",
                    prestoConfig.getBulkConnector(), project, collection);

            Instant now = Instant.now();
            executor.executeRawStatement(format("INSERT INTO %s.%s.%s SELECT * FROM %s WHERE \"$created_at\" <= timestamp '%s'",
                    prestoConfig.getColdStorageConnector(), project, collection, middlewareTable, now.atZone(ZoneId.of("UTC")).format(ISO_LOCAL_DATE))).getResult().thenRun(() -> {
                for (ContinuousQuery continuousQuery : queryMetadataStore.getContinuousQueries(project)) {
                    String query = QueryFormatter.format(continuousQuery.getQuery(), name -> {
                        if (name.getPrefix().map(e -> e.equals("collection")).orElse(true) && name.getSuffix().equals(collection)) {
                            return middlewareTable;
                        } else if (!name.getPrefix().isPresent() && name.getSuffix().equals("_all")) {
                            return middlewareTable;
                        }
                        return executor.formatTableReference(project, name);
                    });

                    executor.executeRawStatement(format("CREATE OR REPLACE VIEW %s.%s.%s AS %s",
                            prestoConfig.getStreamingConnector(), project, collection, query))
                            .getResult().join();
                }
            });
        } catch (SQLException e) {
            throw Throwables.propagate(e);
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

    @Override
    public void store(Event event) {
        try {
            kinesis.putRecord(config.getEventStoreStreamName(), getBuffer(event),
                    event.project() + "|" + event.collection());
        } catch (ResourceNotFoundException e) {
            try {
                createAndWaitForStreamToBecomeAvailable(kinesis, config.getEventStoreStreamName(), 1);
            } catch (Exception e1) {
                throw new RuntimeException("Couldn't send event to Amazon Kinesis", e);
            }
        }
    }

    private ByteBuffer getBuffer(Event event) {
        DatumWriter writer = new FilteredRecordWriter(event.properties().getSchema(), GenericData.get());
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

        if (out.remaining() < 1000) {
            out.position(0);
        }

        return out.getBuffer(startPosition, endPosition - startPosition);
    }
}