package org.rakam.pg9.analysis;

import org.rakam.analysis.ApiKeyService;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.TestApiKeyService;
import org.rakam.pg9.TestingEnvironmentPg9;
import org.rakam.postgresql.PostgresqlApiKeyService;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeSuite;

public class TestPostgresqlApiKeyService extends TestApiKeyService {
    private PostgresqlApiKeyService apiKeyService;

    @BeforeSuite
    public void setupPostgresql() {
        TestingEnvironmentPg9 testingEnvironment = new TestingEnvironmentPg9();
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
