package org.rakam;

import org.rakam.aws.AWSConfig;
import org.rakam.aws.AWSKinesisEventStore;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.plugin.EventStore;

public class TestPrestoKinesisEventExplorer extends TestPrestoEventExplorer {
    private AWSKinesisEventStore testingPrestoEventStore;

    @Override
    public void setupInline() {
        testingPrestoEventStore = new AWSKinesisEventStore(getAWSConfig(), getMetastore(),
                getPrestoQueryExecutor(),  getQueryMetadataStore(),
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
