package org.rakam.ui.user.saml;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import javax.validation.constraints.NotNull;
import java.util.concurrent.TimeUnit;

public class SamlConfig {
    private Duration samlCookieTtl = Duration.succinctDuration(1, TimeUnit.HOURS);
    private String metadata;

    public Duration getSamlCookieTtl() {
        return samlCookieTtl;
    }

    @Config("ui.authentication.saml.cookie-ttl")
    public SamlConfig setSamlCookieTtl(Duration samlCookieTtl) {
        this.samlCookieTtl = samlCookieTtl;
        return this;
    }

    @NotNull
    public String getSamlMetadata() {
        return metadata;
    }

    @Config("ui.authentication.saml.metadata")
    public SamlConfig setSamlMetadata(String metadata) {
        this.metadata = metadata;
        return this;
    }
}
