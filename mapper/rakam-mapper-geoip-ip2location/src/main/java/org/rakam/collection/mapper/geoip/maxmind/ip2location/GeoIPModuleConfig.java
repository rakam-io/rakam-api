package org.rakam.collection.mapper.geoip.maxmind.ip2location;

import io.airlift.configuration.Config;

import java.util.List;

public class GeoIPModuleConfig {
    private List<String> attributes;
    private String databaseUrl = null;

    public String getDatabaseUrl() {
        return databaseUrl;
    }

    @Config("plugin.geoip.database.url")
    public GeoIPModuleConfig setDatabaseUrl(String url) {
        this.databaseUrl = url;
        return this;
    }
}
