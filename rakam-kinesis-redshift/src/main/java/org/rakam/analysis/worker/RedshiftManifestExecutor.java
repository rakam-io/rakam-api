package org.rakam.analysis.worker;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorExecutorBase;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorRecordProcessorFactory;
import com.amazonaws.services.kinesis.model.Record;
import io.airlift.units.Duration;
import org.rakam.analysis.AWSConfig;
import org.rakam.analysis.RedshiftConfig;

import java.util.Properties;

/**
* Created by buremba <Burak Emre Kabakcı> on 16/07/15 19:06.
*/
public class RedshiftManifestExecutor extends KinesisConnectorExecutorBase<Record, Record> {
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
