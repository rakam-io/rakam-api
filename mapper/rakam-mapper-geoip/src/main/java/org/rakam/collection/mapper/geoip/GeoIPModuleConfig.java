package org.rakam.collection.mapper.geoip;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import java.io.File;
import java.util.List;

public class GeoIPModuleConfig {
    private File database;
    private List<String> attributes;
    private String databaseUrl = "http://geolite.maxmind.com/download/geoip/database/GeoLite2-City.mmdb.gz";
    private SourceType source = SourceType.ip_field;

    public enum SourceType {
        request_ip,
        ip_field
    }

    @Config("plugin.geoip.database")
    public GeoIPModuleConfig setDatabase(File database)
    {
        this.database = database;
        return this;
    }

    @Config("plugin.geoip.check-ip-field")
    public GeoIPModuleConfig setSource(SourceType source)
    {
        this.source = source;
        return this;
    }

    public SourceType getSource() {
        return source;
    }

    @Config("plugin.geoip.database.url")
    public GeoIPModuleConfig setDatabaseUrl(String type)
    {
        this.databaseUrl = type;
        return this;
    }

    @Config("plugin.geoip.attributes")
    @ConfigDescription("The list of attributes that will be attached to event. " +
            "Available attributes: country, country_code, region,city, latitude, longitude, timezone")
    public GeoIPModuleConfig setAttributes(String attributes)
    {
        this.attributes = ImmutableList.copyOf(Splitter.on(',').omitEmptyStrings().trimResults().split(attributes));
        return this;
    }

    public File getDatabase() {
        return database;
    }


    public String getDatabaseUrl() {
        return databaseUrl;
    }

    public List<String> getAttributes() {
        return attributes;
    }
}
