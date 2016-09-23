package org.rakam.aws.dynamodb;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.EventBus;
import io.airlift.log.Logger;
import org.junit.Ignore;
import org.rakam.analysis.metadata.AbstractMetastore;
import org.rakam.aws.AWSConfig;
import org.rakam.aws.DynamodbUtil;
import org.rakam.aws.dynamodb.metastore.DynamodbMetastore;
import org.rakam.aws.dynamodb.metastore.DynamodbMetastoreConfig;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.collection.TestMetastore;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.InputStream;

public class TestDynamodbMetastore
//        extends TestMetastore
{
    private final static Logger LOGGER = Logger.get(TestDynamodbMetastore.class);

    private DynamodbMetastore metastore;
    private DynamodbUtil.DynamodbProcess dynamodbProcess;

//    @Override
    public AbstractMetastore getMetastore()
    {
        return metastore;
    }

    @BeforeSuite
    public void tearUp()
            throws Exception
    {
        dynamodbProcess = DynamodbUtil.createDynamodbProcess();
        AWSConfig config = new AWSConfig()
                .setDynamodbEndpoint("http://localhost:" + dynamodbProcess.port)
                .setAccessKey("AKIAIOSFODNN7EXAMPLE")
                .setSecretAccessKey("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        metastore = new DynamodbMetastore(config,
                new DynamodbMetastoreConfig(),
                new FieldDependencyBuilder.FieldDependency(ImmutableSet.of(), ImmutableMap.of()),
                new EventBus());
        metastore.setup();
    }

    @AfterSuite
    public void tearDown()
            throws Exception
    {
        metastore.deleteTable();

        if (!dynamodbProcess.process.isAlive()) {
            LOGGER.error("Dynamodb process exited with %d",
                    dynamodbProcess.process.exitValue());
        }
        else {
            dynamodbProcess.process.destroy();
        }
    }
}
