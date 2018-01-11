package org.rakam.ui.user.saml;

import com.google.inject.Inject;
import org.rakam.ui.AuthService;
import org.rakam.util.RAsyncHttpClient;

public class SamlAuthService implements AuthService {
    RAsyncHttpClient httpClient;

    @Inject
    public SamlAuthService(SamlConfig config) {
        httpClient = RAsyncHttpClient.create(3000, "rakam-api");
    }

    @Override
    public boolean login(String username, String password) {
        // The authentification is done via SAML.
        return false;
    }

    @Override
    public void checkAccess(int userId) {
        // SAML doesn't provide a way to verify the user.
    }
}
