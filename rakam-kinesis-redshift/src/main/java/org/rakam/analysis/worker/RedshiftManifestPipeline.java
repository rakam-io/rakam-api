package org.rakam.analysis.worker;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.impl.AllPassFilter;
import com.amazonaws.services.kinesis.connectors.impl.BasicMemoryBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.*;
import com.amazonaws.services.kinesis.model.Record;
import org.rakam.analysis.AWSConfig;
import org.rakam.analysis.RedshiftManifestEmitter;
import org.rakam.analysis.RedshiftConfig;

import java.io.IOException;
import java.util.Properties;

/**
* Created by buremba <Burak Emre Kabakcı> on 16/07/15 19:05.
*/
public class RedshiftManifestPipeline implements IKinesisConnectorPipeline<Record, Record> {

    private final AWSConfig config;
    private final RedshiftConfig redshiftConfig;

    public RedshiftManifestPipeline(AWSConfig config, RedshiftConfig redshiftConfig) {
        this.config = config;
        this.redshiftConfig = redshiftConfig;
    }

    @Override
    public com.amazonaws.services.kinesis.connectors.interfaces.IEmitter<Record> getEmitter(KinesisConnectorConfiguration configuration) {
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
