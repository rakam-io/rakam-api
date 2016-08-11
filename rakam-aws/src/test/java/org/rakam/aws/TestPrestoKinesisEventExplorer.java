package org.rakam.aws;

import org.rakam.TestPrestoEventExplorer;
import org.rakam.aws.AWSConfig;
import org.rakam.aws.kinesis.AWSKinesisPrestoEventStore;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.plugin.EventStore;

public class TestPrestoKinesisEventExplorer extends TestPrestoEventExplorer
{
    private AWSKinesisPrestoEventStore testingPrestoEventStore;

    @Override
    public void setupInline() {
        testingPrestoEventStore = new AWSKinesisPrestoEventStore(getAWSConfig(), getMetastore(),
                getMetastoreDataSource(),
                getPrestoQueryExecutor(), getContinuousQueryService(),
                getEnvironment().getPrestoConfig(),
                new FieldDependencyBuilder().build());
    }

    public EventStore getEventStore() {
        return testingPrestoEventStore;
    }

    private AWSConfig getAWSConfig() {
        int kinesisPort = super.getEnvironment().getKinesisPort();
        return new AWSConfig().setAccessKey("")
                .setSecretAccessKey("")
                .setRegion("eu-central-1")
                .setKinesisEndpoint(kinesisPort == 0 ? null : "http://127.0.0.1:" + kinesisPort)
                .setEventStoreStreamName("rakam-events");
    }
}
