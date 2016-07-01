package org.rakam.aws.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.util.Base64;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.name.Named;
import io.airlift.log.Logger;
import io.netty.util.CharsetUtil;
import org.apache.avro.generic.FilteredRecordWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.aws.AWSConfig;
import org.rakam.aws.s3.S3BulkEventStore;
import org.rakam.collection.Event;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.SyncEventStore;
import org.rakam.presto.analysis.PrestoConfig;
import org.rakam.presto.analysis.PrestoQueryExecution;
import org.rakam.presto.analysis.PrestoQueryExecutor;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static java.lang.String.format;
import static org.rakam.presto.analysis.PrestoQueryExecution.PRESTO_TIMESTAMP_FORMAT;

public class AWSKinesisPrestoEventStore
        implements SyncEventStore
{
    private final static Logger LOGGER = Logger.get(AWSKinesisPrestoEventStore.class);

    private final AmazonKinesisClient kinesis;
    private final AWSConfig config;
    private static final int BATCH_SIZE = 500;
    private final S3BulkEventStore bulkClient;
    private final PrestoQueryExecutor executor;
    private final PrestoConfig prestoConfig;
    private final ContinuousQueryService continuousQueryService;
    private final JDBCPoolDataSource dataSource;

    private ThreadLocal<KByteArrayOutputStream> buffer = new ThreadLocal<KByteArrayOutputStream>()
    {
        @Override
        protected KByteArrayOutputStream initialValue()
        {
            return new KByteArrayOutputStream(1000000);
        }
    };

    @Inject
    public AWSKinesisPrestoEventStore(AWSConfig config,
            Metastore metastore,
            @Named("presto.metastore.jdbc") JDBCPoolDataSource dataSource,
            PrestoQueryExecutor executor,
            ContinuousQueryService continuousQueryService,
            PrestoConfig prestoConfig,
            FieldDependencyBuilder.FieldDependency fieldDependency)
    {
        kinesis = new AmazonKinesisClient(config.getCredentials());
        kinesis.setRegion(config.getAWSRegion());
        if (config.getKinesisEndpoint() != null) {
            kinesis.setEndpoint(config.getKinesisEndpoint());
        }
        this.config = config;
        this.executor = executor;
        this.prestoConfig = prestoConfig;
        this.dataSource = dataSource;
        this.continuousQueryService = continuousQueryService;
        this.bulkClient = new S3BulkEventStore(metastore, config, fieldDependency);

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
        String project = events.get(0).project();
        bulkClient.upload(project, events);
    }

    @Override
    public QueryExecution commit(String project, String collection)
    {
        Instant now = Instant.now();

        Connection conn;
        try {
            conn = dataSource.getConnection();

            String lockKey = "bulk." + project + "." + collection;
            ResultSet rs = conn.createStatement().executeQuery(format("SELECT GET_LOCK('%s', 10)", lockKey));
            rs.next();
            if (rs.getInt(1) != 1) {
                throw new RakamException(StandardErrors.CONFLICT_COMMIT_MESSAGE, CONFLICT);
            }

            String middlewareTable = format("FROM %s.\"%s\".\"%s\" WHERE \"$created_at\" < timestamp '%s UTC'",
                    prestoConfig.getBulkConnector(), project, collection,
                    PRESTO_TIMESTAMP_FORMAT.format(now.atZone(ZoneOffset.UTC)));

            QueryExecution insertQuery = executor.executeRawStatement(format("INSERT INTO %s.\"%s\".\"%s\" SELECT * %s",
                    prestoConfig.getColdStorageConnector(), project, collection, middlewareTable));

            return new ChainQueryExecution(ImmutableList.of(insertQuery), null, (results) -> {
                if (results.get(0).isFailed()) {
                    return insertQuery;
                }
                ImmutableList.Builder<QueryExecution> builder = ImmutableList.builder();
                for (ContinuousQuery continuousQuery : continuousQueryService.list(project)) {
                    AtomicBoolean ref = new AtomicBoolean();

                    String query = QueryFormatter.format(continuousQuery.getQuery(), name -> {
                        if ((name.getPrefix().map(e -> e.equals("collection")).orElse(true) && name.getSuffix().equals(collection)) ||
                                !name.getPrefix().isPresent() && name.getSuffix().equals("_all")) {
                            ref.set(true);
                            return format("(SELECT '%s' as \"$collection\", * ", collection) + middlewareTable + ")";
                        }
                        return executor.formatTableReference(project, name);
                    }, '"');

                    if (!ref.get()) {
                        continue;
                    }

                    PrestoQueryExecution processQuery = executor.executeRawQuery(format("CREATE OR REPLACE VIEW %s.\"%s\".\"%s\" AS %s",
                            prestoConfig.getStreamingConnector(), project, continuousQuery.tableName, query),
                            ImmutableMap.of(prestoConfig.getStreamingConnector() + ".append_data", "true"),
                            prestoConfig.getStreamingConnector());
                    builder.add(processQuery);
                }

                return new ChainQueryExecution(builder.build(), null, (viewUpdateResults) -> {
                    List<QueryError> continuousQueryErrors = viewUpdateResults.stream()
                            .filter(e -> e.isFailed()).map(e -> e.getError())
                            .collect(Collectors.toList());

                    if (!continuousQueryErrors.isEmpty()) {
                        LOGGER.warn("Error while updating continuous queries via bulk commit: " + continuousQueryErrors);
                    }

                    DelegateQueryExecution clearStaging = new DelegateQueryExecution(executor.executeRawStatement(format("DELETE FROM %s.%s.%s WHERE \"$created_at\" <= timestamp '%s UTC'", prestoConfig.getBulkConnector(),
                            project, collection, PRESTO_TIMESTAMP_FORMAT.format(now.atZone(ZoneOffset.UTC)))), result -> {
                        if (!continuousQueryErrors.isEmpty()) {
                            result.setProperty("continuousQueryUpdateError", continuousQueryErrors);
                        }
                        return result;
                    });

                    try {
                        conn.createStatement().execute(format("SELECT RELEASE_LOCK('%s')", lockKey));
                        conn.close();
                    }
                    catch (SQLException e1) {
                    }

                    return clearStaging;
                });
            });
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
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

    @Override
    public QueryExecution copy(String project, String collection, List<URL> urls, CopyType type, CompressionType compressionType, Map<String, String> options)
    {
        if(type == null) {
            throw new RakamException("source type is missing", BAD_REQUEST);
        }

        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

        builder.put(format("%s.urls", prestoConfig.getBulkConnector()),
                Base64.encodeAsString(JsonHelper.encodeAsBytes(urls)));

        builder.put(format("%s.source_type", prestoConfig.getBulkConnector()), type.name());

        if (options != null) {
            builder.put(format("%s.source_options", prestoConfig.getBulkConnector()),
                    Base64.encodeAsString(JsonHelper.encode(options).getBytes(CharsetUtil.UTF_8)));
        }

        if (compressionType != null) {
            builder.put(format("%s.compression", prestoConfig.getBulkConnector()), type.name());
        }

        return executor.executeRawQuery(format("insert into %s.%s.%s select * from %s.%s.%s",
                prestoConfig.getColdStorageConnector(), project, ValidationUtil.checkCollection(collection),
                prestoConfig.getBulkConnector(), project, ValidationUtil.checkCollection(collection)),
                builder.build(), "middleware");
    }
}