package org.rakam.pg10.analysis;

import org.rakam.analysis.ApiKeyService;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.TestApiKeyService;
import org.rakam.pg10.TestingEnvironmentPg10;
import org.rakam.postgresql.PostgresqlApiKeyService;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeSuite;

public class TestPostgresqlApiKeyService extends TestApiKeyService {
    private PostgresqlApiKeyService apiKeyService;

    @BeforeSuite
    public void setupPostgresql() {
        TestingEnvironmentPg10 testingEnvironment = new TestingEnvironmentPg10();
        JDBCPoolDataSource apiKeyServiceDataSource = JDBCPoolDataSource
                .getOrCreateDataSource(testingEnvironment.getPostgresqlConfig());

        apiKeyService = new PostgresqlApiKeyService(apiKeyServiceDataSource);
        apiKeyService.setup();
    }

    @Override
    public ApiKeyService getApiKeyService() {
        return apiKeyService;
    }

    @AfterMethod
    public void tearDownMethod() throws Exception {
        apiKeyService.clearCache();
    }

}
