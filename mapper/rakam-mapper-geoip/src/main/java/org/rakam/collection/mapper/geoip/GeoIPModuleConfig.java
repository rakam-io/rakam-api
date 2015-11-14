package org.rakam.collection.mapper.geoip;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import java.util.List;

public class GeoIPModuleConfig {
    private List<String> attributes;
    private String databaseUrl = "http://geolite.maxmind.com/download/geoip/database/GeoLite2-City.mmdb.gz";
    private String ispDatabaseUrl;
    private String connectionTypeDatabaseUrl;
    private boolean useExistingFields;

    @Config("plugin.geoip.database.url")
    public GeoIPModuleConfig setDatabaseUrl(String type)
    {
        this.databaseUrl = type;
        return this;
    }
    @Config("plugin.geoip.isp-database.url")
    public GeoIPModuleConfig setIspDatabaseUrl(String type)
    {
        this.ispDatabaseUrl = type;
        return this;
    }

    public String getIspDatabaseUrl() {
        return ispDatabaseUrl;
    }

    @Config("plugin.geoip.connection-type-database.url")
    public GeoIPModuleConfig setConnectionTypeDatabaseUrl(String type)
    {
        this.connectionTypeDatabaseUrl = type;
        return this;
    }

    public String getConnectionTypeDatabaseUrl() {
        return connectionTypeDatabaseUrl;
    }

    @Config("plugin.geoip.attributes")
    @ConfigDescription("The list of attributes that will be attached to event. " +
            "Available attributes: country, country_code, region,city, latitude, longitude, timezone")
    public GeoIPModuleConfig setAttributes(String attributes)
    {
        this.attributes = ImmutableList.copyOf(Splitter.on(',').omitEmptyStrings().trimResults().split(attributes));
        return this;
    }

    public String getDatabaseUrl() {
        return databaseUrl;
    }

    public List<String> getAttributes() {
        return attributes;
    }

    @Config("plugin.geoip.use-existing-fields")
    @ConfigDescription("Use existing fields  " +
            "Available attributes: country, country_code, region,city, latitude, longitude, timezone")
    public GeoIPModuleConfig setUseExistingFields(boolean useExistingFields)
    {
        this.useExistingFields = useExistingFields;
        return this;
    }

    public boolean getUseExistingFields() {
        return useExistingFields;
    }
}
