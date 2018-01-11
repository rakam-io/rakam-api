package org.rakam.module.website;

import io.airlift.configuration.Config;

public class WebsiteMapperConfig {
    private boolean userAgent = true;
    private boolean referrer = true;
    private boolean trackSpiders = false;

    public boolean getReferrer() {
        return referrer;
    }

    @Config("module.website.mapper.referrer")
    public WebsiteMapperConfig setReferrer(boolean referrer) {
        this.referrer = true;
        return this;
    }

    public boolean getUserAgent() {
        return userAgent;
    }

    @Config("module.website.mapper.user-agent")
    public WebsiteMapperConfig setUserAgent(boolean enabled) {
        this.userAgent = enabled;
        return this;
    }

    public boolean getTrackSpiders() {
        return trackSpiders;
    }

    @Config("module.website.mapper.user_agent.track_spiders")
    public WebsiteMapperConfig setTrackSpiders(boolean trackSpiders) {
        this.trackSpiders = true;
        return this;
    }
}
