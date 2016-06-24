package org.rakam.analysis;

import org.rakam.TestingEnvironment;
import org.rakam.postgresql.PostgresqlApiKeyService;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;

public class TestPostgresqlApiKeyService extends TestApiKeyService
{
    private PostgresqlApiKeyService apiKeyService;

    @BeforeSuite
    public void setup() {
        TestingEnvironment testingEnvironment = new TestingEnvironment();
        JDBCPoolDataSource apiKeyServiceDataSource = JDBCPoolDataSource
                .getOrCreateDataSource(testingEnvironment.getPostgresqlConfig());

        apiKeyService = new PostgresqlApiKeyService(apiKeyServiceDataSource);
        apiKeyService.setup();

    }

    @Override
    public ApiKeyService getApiKeyService()
    {
        return apiKeyService;
    }

    @AfterMethod
    public void tearDownMethod() throws Exception {
        apiKeyService.clearCache();
    }

}
