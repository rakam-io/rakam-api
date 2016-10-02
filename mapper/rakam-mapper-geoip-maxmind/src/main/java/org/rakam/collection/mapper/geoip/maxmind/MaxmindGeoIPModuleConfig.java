package org.rakam.collection.mapper.geoip.maxmind;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import java.util.List;

public class MaxmindGeoIPModuleConfig
{
    private List<String> attributes;
    private String databaseUrl = "http://geolite.maxmind.com/download/geoip/database/GeoLite2-City.mmdb.gz";
    private String ispDatabaseUrl;
    private String connectionTypeDatabaseUrl;
    private boolean useExistingFields;

    @Config("plugin.geoip.database.url")
    public MaxmindGeoIPModuleConfig setDatabaseUrl(String url)
    {
        this.databaseUrl = url;
        return this;
    }
    @Config("plugin.geoip.isp-database.url")
    public MaxmindGeoIPModuleConfig setIspDatabaseUrl(String url)
    {
        this.ispDatabaseUrl = url;
        return this;
    }

    public String getIspDatabaseUrl() {
        return ispDatabaseUrl;
    }

    @Config("plugin.geoip.connection-type-database.url")
    public MaxmindGeoIPModuleConfig setConnectionTypeDatabaseUrl(String type)
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
    public MaxmindGeoIPModuleConfig setAttributes(String attributes)
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
    public MaxmindGeoIPModuleConfig setUseExistingFields(boolean useExistingFields)
    {
        this.useExistingFields = useExistingFields;
        return this;
    }

    public boolean getUseExistingFields() {
        return useExistingFields;
    }
}
