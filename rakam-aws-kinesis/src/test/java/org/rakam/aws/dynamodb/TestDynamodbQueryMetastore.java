package org.rakam.aws.dynamodb;

import org.rakam.analysis.TestQueryMetastore;
import org.rakam.analysis.metadata.QueryMetadataStore;
import org.rakam.aws.AWSConfig;
import org.rakam.aws.dynamodb.apikey.DynamodbApiKeyConfig;
import org.rakam.aws.dynamodb.apikey.DynamodbApiKeyService;
import org.rakam.aws.dynamodb.metastore.DynamodbQueryMetastore;
import org.rakam.aws.dynamodb.metastore.DynamodbQueryMetastoreConfig;
import org.testng.annotations.BeforeSuite;

public class TestDynamodbQueryMetastore extends TestQueryMetastore
{
    private final DynamodbQueryMetastore service;
    private Process dynamodbServer;

    public TestDynamodbQueryMetastore()
            throws Exception
    {
//        int dynamodb = createDynamodb();
        int dynamodb = 8000;
        service = new DynamodbQueryMetastore(new AWSConfig()
                .setAccessKey("test")
                .setSecretAccessKey("test")
                .setDynamodbEndpoint("http://127.0.0.1:" + dynamodb),
                new DynamodbQueryMetastoreConfig().setTableName("query-metastore"));
    }

    @BeforeSuite
    public void setUp()
            throws Exception
    {
        service.setup();
    }

    @Override
    public QueryMetadataStore getQuerymetastore()
    {
        return service;
    }
}
