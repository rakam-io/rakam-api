package org.rakam.module.website;

import io.airlift.configuration.Config;

public class WebsiteMapperConfig {
    private boolean userAgent = true;
    private boolean referrer = true;

    @Config("module.website.mapper.user-agent")
    public WebsiteMapperConfig setUserAgent(boolean enabled) {
        this.userAgent = enabled;
        return this;
    }

    public boolean getReferrer() {
        return referrer;
    }

    public boolean getUserAgent()
    {
        return userAgent;
    }

    @Config("module.website.mapper.referrer")
    public WebsiteMapperConfig setReferrer(boolean referrer)
    {
        this.referrer = true;
        return this;
    }
}
