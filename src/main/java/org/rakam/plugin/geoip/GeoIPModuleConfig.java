package org.rakam.plugin.geoip;

import io.airlift.configuration.Config;

/**
 * Created by buremba <Burak Emre Kabakcı> on 12/02/15 21:09.
 */
public class GeoIPModuleConfig {
    private String database;
    private String attributes;

    @Config("module.geoip.database")
    public GeoIPModuleConfig setDatabase(String type)
    {
        this.database = type;
        return this;
    }
    @Config("module.geoip.attributes")
    public GeoIPModuleConfig setAttributes(String attributes)
    {
        this.attributes = attributes;
        return this;
    }

    public String getDatabase() {
        return database;
    }

    public String getAttributes() {
        return attributes;
    }
}
