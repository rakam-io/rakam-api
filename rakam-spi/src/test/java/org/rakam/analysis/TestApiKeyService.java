package org.rakam.analysis;

import org.rakam.analysis.ApiKeyService.AccessKeyType;
import org.rakam.util.RakamException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public abstract class TestApiKeyService {
    private static final String PROJECT_NAME = TestApiKeyService.class.getName().replace(".", "_").toLowerCase();

    public abstract ApiKeyService getApiKeyService();

    @AfterMethod
    public void tearDownMethod() throws Exception {
        getApiKeyService().revokeAllKeys(PROJECT_NAME);
    }

    @Test
    public void testCreateApiKeys() throws Exception {
        ApiKeyService.ProjectApiKeys testing = getApiKeyService().createApiKeys(PROJECT_NAME);

        assertEquals(getApiKeyService().getProjectOfApiKey(testing.readKey(), AccessKeyType.READ_KEY), PROJECT_NAME);
        assertEquals(getApiKeyService().getProjectOfApiKey(testing.writeKey(), AccessKeyType.WRITE_KEY), PROJECT_NAME);
        assertEquals(getApiKeyService().getProjectOfApiKey(testing.masterKey(), AccessKeyType.MASTER_KEY), PROJECT_NAME);
    }

    @Test
    public void testInvalidApiKeys() throws Exception {
        getApiKeyService().createApiKeys(PROJECT_NAME);

        try {
            getApiKeyService().getProjectOfApiKey("invalidKey", AccessKeyType.READ_KEY);
            fail();
        } catch (RakamException e) {
        }

        try {
            getApiKeyService().getProjectOfApiKey("invalidKey", AccessKeyType.WRITE_KEY);
            fail();
        } catch (RakamException e) {
        }

        try {
            getApiKeyService().getProjectOfApiKey("invalidKey", AccessKeyType.MASTER_KEY);
            fail();
        } catch (RakamException e) {
        }
    }

    @Test
    public void testRevokeApiKeys() throws Exception {
        ApiKeyService.ProjectApiKeys apiKeys = getApiKeyService().createApiKeys(PROJECT_NAME);

        getApiKeyService().revokeApiKeys(PROJECT_NAME, apiKeys.masterKey());

        try {
            getApiKeyService().getProjectOfApiKey(apiKeys.readKey(), AccessKeyType.READ_KEY);
            fail();
        } catch (RakamException e) {
        }

        try {
            getApiKeyService().getProjectOfApiKey(apiKeys.writeKey(), AccessKeyType.WRITE_KEY);
            fail();
        } catch (RakamException e) {
        }

        try {
            getApiKeyService().getProjectOfApiKey(apiKeys.masterKey(), AccessKeyType.MASTER_KEY);
            fail();
        } catch (RakamException e) {
        }
    }
}
