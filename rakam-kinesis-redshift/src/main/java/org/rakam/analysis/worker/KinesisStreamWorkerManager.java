package org.rakam.analysis.worker;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.inject.Inject;
import org.rakam.analysis.AWSConfig;
import org.rakam.collection.event.metastore.Metastore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.rakam.analysis.util.aws.KinesisUtils;
import org.rakam.analysis.RedshiftConfig;
import org.rakam.analysis.util.aws.S3Utils;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
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
    private final ExecutorService executorService;
    private final AmazonKinesisClient kinesisClient;
    private final RedshiftConfig redshiftConfig;
    private ScheduledExecutorService scheduler = null;

    @Inject
    public KinesisStreamWorkerManager(AWSConfig config, RedshiftConfig redshiftConfig, Metastore metastore) {
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
    public void initializeWorker() {
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
            value = new Thread(new S3ManifestConnectorExecutor(config, metastore, config.getKinesisStream(), config.getS3Bucket()));
        } catch (Exception e) {
            LOGGER.error("Error creating Kinesis stream worker", e);
            return;
        }
        LOGGER.info("Created Kinesis stream worker.");
        value.start();

        Thread redshiftExecutor = new Thread(new RedshiftManifestExecutor(config, redshiftConfig));
        redshiftExecutor.start();
    }


}
