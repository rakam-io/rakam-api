package org.rakam.aws.dynamodb;

import org.rakam.analysis.ConfigManager;
import org.rakam.analysis.TestConfigManager;
import org.rakam.aws.AWSConfig;
import org.rakam.aws.dynamodb.config.DynamodbConfigManager;
import org.rakam.aws.dynamodb.config.DynamodbConfigManagerConfig;
import org.testng.annotations.BeforeSuite;

public class TestDynamodbConfigManager
        extends TestConfigManager
{
    private final DynamodbConfigManager service;
    private Process dynamodbServer;

    public TestDynamodbConfigManager()
    {
        //        int dynamodb = createDynamodb();
        int dynamodb = 8000;
        service = new DynamodbConfigManager(new AWSConfig()
                .setAccessKey("test")
                .setSecretAccessKey("test")
                .setDynamodbEndpoint("http://127.0.0.1:" + dynamodb),
                new DynamodbConfigManagerConfig().setTableName("config-manager"));

    }

    @BeforeSuite
    public void setUp()
            throws Exception
    {
        service.setup();
    }

    @Override
    public ConfigManager getConfigManager()
    {
        return service;
    }
}
