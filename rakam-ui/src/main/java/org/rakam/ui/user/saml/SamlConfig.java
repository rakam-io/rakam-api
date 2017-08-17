package org.rakam.ui.user.saml;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import javax.validation.constraints.NotNull;

public class SamlConfig
{
    private Duration samlCacheTtl;
    private String metadata;

    public Duration getSamlCacheTtl()
    {
        return samlCacheTtl;
    }

    @Config("ui.authentication.saml.cache-ttl")
    public SamlConfig setSamlCacheTtl(Duration ldapCacheTtl)
    {
        this.samlCacheTtl = ldapCacheTtl;
        return this;
    }

    @NotNull
    public String getSamlMetadata()
    {
        return metadata;
    }

    @Config("ui.authentication.saml.metadata")
    public SamlConfig setSamlMetadata(String metadata)
    {
        this.metadata = metadata;
        return this;
    }
}
