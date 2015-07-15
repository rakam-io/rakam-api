package org.rakam.analysis;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by buremba <Burak Emre Kabakcı> on 06/07/15 04:27.
 */
public abstract class KinesisExecutorBase implements Runnable {
    private static final Log LOG = LogFactory.getLog(KinesisExecutorBase.class);

    // Amazon Kinesis Client Library worker to process records
    protected Worker worker;

    /**
     * Initialize the Amazon Kinesis Client Library configuration and worker
     *
     * @param kinesisConnectorConfiguration Amazon Kinesis connector configuration
     */
    protected void initialize(KinesisConnectorConfiguration kinesisConnectorConfiguration) {
        initialize(kinesisConnectorConfiguration, null);
    }

    /**
     * Initialize the Amazon Kinesis Client Library configuration and worker with metrics factory
     *
     * @param kinesisConnectorConfiguration Amazon Kinesis connector configuration
     * @param metricFactory would be used to emit metrics in Amazon Kinesis Client Library
     */
    protected void
    initialize(KinesisConnectorConfiguration kinesisConnectorConfiguration, IMetricsFactory metricFactory) {
        KinesisClientLibConfiguration kinesisClientLibConfiguration =
                new KinesisClientLibConfiguration(kinesisConnectorConfiguration.APP_NAME,
                        kinesisConnectorConfiguration.KINESIS_INPUT_STREAM,
                        kinesisConnectorConfiguration.AWS_CREDENTIALS_PROVIDER,
                        kinesisConnectorConfiguration.WORKER_ID).withKinesisEndpoint(kinesisConnectorConfiguration.KINESIS_ENDPOINT)
                        .withFailoverTimeMillis(kinesisConnectorConfiguration.FAILOVER_TIME)
                        .withMaxRecords(kinesisConnectorConfiguration.MAX_RECORDS)
                        .withInitialPositionInStream(kinesisConnectorConfiguration.INITIAL_POSITION_IN_STREAM)
                        .withIdleTimeBetweenReadsInMillis(kinesisConnectorConfiguration.IDLE_TIME_BETWEEN_READS)
                        .withCallProcessRecordsEvenForEmptyRecordList(KinesisConnectorConfiguration.DEFAULT_CALL_PROCESS_RECORDS_EVEN_FOR_EMPTY_LIST)
                        .withCleanupLeasesUponShardCompletion(kinesisConnectorConfiguration.CLEANUP_TERMINATED_SHARDS_BEFORE_EXPIRY)
                        .withParentShardPollIntervalMillis(kinesisConnectorConfiguration.PARENT_SHARD_POLL_INTERVAL)
                        .withShardSyncIntervalMillis(kinesisConnectorConfiguration.SHARD_SYNC_INTERVAL)
                        .withTaskBackoffTimeMillis(kinesisConnectorConfiguration.BACKOFF_INTERVAL)
                        .withMetricsBufferTimeMillis(kinesisConnectorConfiguration.CLOUDWATCH_BUFFER_TIME)
                        .withMetricsMaxQueueSize(kinesisConnectorConfiguration.CLOUDWATCH_MAX_QUEUE_SIZE)
                        .withUserAgent(kinesisConnectorConfiguration.APP_NAME + ","
                                + kinesisConnectorConfiguration.CONNECTOR_DESTINATION + ","
                                + KinesisConnectorConfiguration.KINESIS_CONNECTOR_USER_AGENT)
                        .withRegionName(kinesisConnectorConfiguration.REGION_NAME);

        if (!kinesisConnectorConfiguration.CALL_PROCESS_RECORDS_EVEN_FOR_EMPTY_LIST) {
            LOG.warn("The false value of callProcessRecordsEvenForEmptyList will be ignored. It must be set to true for the bufferTimeMillisecondsLimit to work correctly.");
        }

        if (kinesisConnectorConfiguration.IDLE_TIME_BETWEEN_READS > kinesisConnectorConfiguration.BUFFER_MILLISECONDS_LIMIT) {
            LOG.warn("idleTimeBetweenReads is greater than bufferTimeMillisecondsLimit. For best results, ensure that bufferTimeMillisecondsLimit is more than or equal to idleTimeBetweenReads ");
        }

        // If a metrics factory was specified, use it.
        if (metricFactory != null) {
            worker =
                    new Worker(getKinesisConnectorRecordProcessorFactory(),
                            kinesisClientLibConfiguration,
                            metricFactory);
        } else {
            worker = new Worker(getKinesisConnectorRecordProcessorFactory(), kinesisClientLibConfiguration);
        }
        LOG.info(getClass().getSimpleName() + " worker created");
    }

    @Override
    public void run() {
        if (worker != null) {
            // Start Amazon Kinesis Client Library worker to process records
            LOG.info("Starting worker in " + getClass().getSimpleName());
            try {
                worker.run();
            } catch (Throwable t) {
                LOG.error(t);
                throw t;
            } finally {
                LOG.error("Worker " + getClass().getSimpleName() + " is not running.");
            }
        } else {
            throw new RuntimeException("Initialize must be called before run.");
        }
    }

    /**
     * This method returns a {@link com.amazonaws.services.kinesis.connectors.KinesisConnectorRecordProcessorFactory} that contains the
     * appropriate {@link com.amazonaws.services.kinesis.connectors.interfaces.IKinesisConnectorPipeline} for the Amazon Kinesis Enabled Application
     *
     * @return a {@link com.amazonaws.services.kinesis.connectors.KinesisConnectorRecordProcessorFactory} that contains the appropriate
     *         {@link com.amazonaws.services.kinesis.connectors.interfaces.IKinesisConnectorPipeline} for the Amazon Kinesis Enabled Application
     */
    public abstract IRecordProcessorFactory getKinesisConnectorRecordProcessorFactory();
}
