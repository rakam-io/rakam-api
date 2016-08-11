package org.rakam.aws.dynamodb;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.EventBus;
import io.airlift.log.Logger;
import org.rakam.analysis.metadata.AbstractMetastore;
import org.rakam.aws.AWSConfig;
import org.rakam.aws.DynamodbUtil;
import org.rakam.aws.dynamodb.metastore.DynamodbMetastore;
import org.rakam.aws.dynamodb.metastore.DynamodbMetastoreConfig;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.collection.TestMetastore;
import org.testng.annotations.AfterSuite;

public class TestDynamodbMetastore
        extends TestMetastore
{
    private final DynamodbMetastore metastore;
    private final DynamodbUtil.DynamodbProcess dynamodbProcess;

    public TestDynamodbMetastore()
            throws Exception
    {
        dynamodbProcess = DynamodbUtil.createDynamodbProcess();
        AWSConfig config = new AWSConfig()
                .setDynamodbEndpoint("http://127.0.0.1:" + dynamodbProcess.port)
                .setAccessKey("test")
                .setSecretAccessKey("test");
        metastore = new DynamodbMetastore(config,
                new DynamodbMetastoreConfig(),
                new FieldDependencyBuilder.FieldDependency(ImmutableSet.of(), ImmutableMap.of()),
                new EventBus());
        metastore.setup();
    }

    @Override
    public AbstractMetastore getMetastore()
    {
        return metastore;
    }

    @AfterSuite
    public void tearDown()
            throws Exception
    {
        metastore.deleteTable();
        dynamodbProcess.process.destroy();
    }
}
