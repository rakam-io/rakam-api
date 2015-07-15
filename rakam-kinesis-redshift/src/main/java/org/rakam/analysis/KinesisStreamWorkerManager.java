package org.rakam.analysis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorExecutorBase;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorRecordProcessorFactory;
import com.amazonaws.services.kinesis.connectors.impl.AllPassFilter;
import com.amazonaws.services.kinesis.connectors.impl.BasicMemoryBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import com.amazonaws.services.kinesis.connectors.interfaces.IFilter;
import com.amazonaws.services.kinesis.connectors.interfaces.IKinesisConnectorPipeline;
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.plugin.ContinuousQueryService;
import org.rakam.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redshift.KinesisUtils;
import redshift.RedshiftConfig;
import redshift.S3ManifestConnectorExecutor;
import redshift.S3Utils;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Created by buremba <Burak Emre Kabakcı> on 03/07/15 08:27.
 */
public class KinesisStreamWorkerManager {
    final static Logger LOGGER = LoggerFactory.getLogger(KinesisStreamWorkerManager.class);

    private final Metastore metastore;
    private final AWSConfig config;
    private final Map<Tuple<String, String>, Thread> workers;
    private final ExecutorService executorService;
    private final AmazonKinesisClient kinesisClient;
    private final ContinuousQueryService continuousQueryService;
    private final RedshiftConfig redshiftConfig;
    private ScheduledExecutorService scheduler = null;

    @Inject
    public KinesisStreamWorkerManager(AWSConfig config, RedshiftConfig redshiftConfig, Metastore metastore, ContinuousQueryService continuousQueryService) {
        this.continuousQueryService = continuousQueryService;
        this.workers = new HashMap<>();
        this.config = config;
        this.redshiftConfig = redshiftConfig;
        this.metastore = metastore;
        this.executorService = Executors.newCachedThreadPool();
        StaticCredentialsProvider credentialProvider =
                new StaticCredentialsProvider(new BasicAWSCredentials(config.getAccessKey(), config.getSecretAccessKey()));
        this.kinesisClient = new AmazonKinesisClient(credentialProvider);
        S3Utils.createBucket(new AmazonS3Client(credentialProvider), config.getS3Bucket());
        KinesisUtils.createAndWaitForStreamToBecomeAvailable(kinesisClient, config.getManifestStreamName(), 1);
        KinesisUtils.createAndWaitForStreamToBecomeAvailable(kinesisClient, config.getKinesisStream(), 1);

    }

    @PostConstruct()
    public void initializeWorkers() {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Could not load PostgreSQL driver");
        }

        if(scheduler != null) {
            throw new IllegalStateException("workers are already initialized");
        }
        createWorker();
    }

    @PreDestroy()
    public void destroyWorkers() {
        executorService.shutdown();
        scheduler.shutdown();
    }

    private void createWorker() {

        Thread value;
        try {
            value = new Thread(new S3ManifestConnectorExecutor(config, metastore, config.getKinesisStream(), continuousQueryService, config.getS3Bucket()));
        } catch (Exception e) {
            LOGGER.error("Error creating Kinesis stream worker", e);
            return;
        }
        LOGGER.info("Created Kinesis stream worker.");
        value.start();

        Thread redshiftExecutor = new Thread(new RedshiftManifestExecutor(config, redshiftConfig));
        redshiftExecutor.start();
    }


    public static class RedshiftManifestExecutor extends KinesisConnectorExecutorBase<Record, Record> {
        private final AWSConfig config;
        private final RedshiftConfig redshiftConfig;

        public RedshiftManifestExecutor(AWSConfig config, RedshiftConfig redshiftConfig) {
            this.config = config;
            this.redshiftConfig = redshiftConfig;

            Properties properties = new Properties();
            properties.setProperty(KinesisConnectorConfiguration.PROP_APP_NAME, "rakam-kinesis-consumer");
            properties.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT, Integer.toString(1024 * 50));
            properties.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_RECORD_COUNT_LIMIT, Integer.toString(100));
            properties.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_MILLISECONDS_LIMIT, Long.toString(Duration.valueOf("1m").toMillis()));
            properties.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_INPUT_STREAM, config.getManifestStreamName());

            super.initialize(new KinesisConnectorConfiguration(properties, getAWSCredentialsProvider()));
        }

        @Override
        public KinesisConnectorRecordProcessorFactory<Record, Record> getKinesisConnectorRecordProcessorFactory() {
            Properties properties = new Properties();
            properties.setProperty(KinesisConnectorConfiguration.PROP_APP_NAME, "rakam-kinesis-consumer");
            properties.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT, Integer.toString(1024 * 1024 * 50));
            properties.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_RECORD_COUNT_LIMIT, Integer.toString(10000));
            properties.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_MILLISECONDS_LIMIT, Long.toString(Duration.valueOf("1m").toMillis()));
            properties.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_INPUT_STREAM, config.getManifestStreamName());

            return new KinesisConnectorRecordProcessorFactory<>(new RedshiftManifestPipeline(config, redshiftConfig),
                    new KinesisConnectorConfiguration(properties, getAWSCredentialsProvider()));
        }

        public AWSCredentialsProvider getAWSCredentialsProvider() {
            return new StaticCredentialsProvider(new BasicAWSCredentials(config.getAccessKey(), config.getSecretAccessKey()));
        }
    }

    public static class RedshiftManifestPipeline implements IKinesisConnectorPipeline<Record, Record> {

        private final AWSConfig config;
        private final RedshiftConfig redshiftConfig;

        public RedshiftManifestPipeline(AWSConfig config, RedshiftConfig redshiftConfig) {
            this.config = config;
            this.redshiftConfig = redshiftConfig;
        }

        @Override
        public IEmitter<Record> getEmitter(KinesisConnectorConfiguration configuration) {
            Properties properties = new Properties();
            properties.setProperty(KinesisConnectorConfiguration.PROP_REDSHIFT_DATA_DELIMITER, "|");
            properties.setProperty(KinesisConnectorConfiguration.PROP_REDSHIFT_URL, redshiftConfig.getUrl());
            properties.setProperty(KinesisConnectorConfiguration.PROP_REDSHIFT_USERNAME, redshiftConfig.getUsername());
            properties.setProperty(KinesisConnectorConfiguration.PROP_REDSHIFT_PASSWORD, redshiftConfig.getPassword());
            properties.setProperty(KinesisConnectorConfiguration.PROP_S3_BUCKET, config.getS3Bucket());
            return new RedshiftManifestEmitter(new KinesisConnectorConfiguration(properties, getAWSCredentialsProvider()));
        }

        public AWSCredentialsProvider getAWSCredentialsProvider() {
            return new StaticCredentialsProvider(new BasicAWSCredentials(config.getAccessKey(), config.getSecretAccessKey()));
        }

        @Override
        public IBuffer<Record> getBuffer(KinesisConnectorConfiguration configuration) {
            return new BasicMemoryBuffer<>(configuration);
        }

        @Override
        public ITransformer<Record, Record> getTransformer(KinesisConnectorConfiguration configuration) {
            return new ITransformer<Record, Record>() {
                @Override
                public Record toClass(Record record) throws IOException {
                    return record;
                }

                @Override
                public Record fromClass(Record record) throws IOException {
                    return record;
                }
            };
        }

        @Override
        public IFilter<Record> getFilter(KinesisConnectorConfiguration configuration) {
            return new AllPassFilter<>();
        }
    }
}
