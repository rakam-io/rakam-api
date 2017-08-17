package org.rakam.ui.user.saml;

import com.google.inject.Inject;
import org.rakam.ui.AuthService;

public class SamlAuthService implements AuthService
{
    private final SamlConfig config;

    @Inject
    public SamlAuthService(SamlConfig config)
    {
        this.config = config;
    }

    @Override
    public boolean login(String username, String password)
    {
        return false;
    }

    @Override
    public void checkAccess(int userId)
    {
    }
}
