package org.rakam.collection.mapper.geoip.maxmind;

import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

public class MaxmindGeoIPModuleConfig {

    private static final URL DEMO_URL;

    static {
        try {
            DEMO_URL = new URL("http://geolite.maxmind.com/download/geoip/database/GeoLite2-City.mmdb.gz");
        } catch (MalformedURLException e) {
            throw Throwables.propagate(e);
        }
    }

    private List<String> attributes;
    private URL databaseUrl = DEMO_URL;
    private URL ispDatabaseUrl;
    private URL connectionTypeDatabaseUrl;
    private boolean useExistingFields;

    public URL getIspDatabaseUrl() {
        return ispDatabaseUrl;
    }

    @Config("plugin.geoip.isp-database.url")
    public MaxmindGeoIPModuleConfig setIspDatabaseUrl(URL url) {
        this.ispDatabaseUrl = url;
        return this;
    }

    public URL getConnectionTypeDatabaseUrl() {
        return connectionTypeDatabaseUrl;
    }

    @Config("plugin.geoip.connection-type-database.url")
    public MaxmindGeoIPModuleConfig setConnectionTypeDatabaseUrl(URL url) {
        this.connectionTypeDatabaseUrl = url;
        return this;
    }

    public URL getDatabaseUrl() {
        return databaseUrl;
    }

    @Config("plugin.geoip.database.url")
    public MaxmindGeoIPModuleConfig setDatabaseUrl(URL url) {
        this.databaseUrl = url;
        return this;
    }

    public List<String> getAttributes() {
        return attributes;
    }

    @Config("plugin.geoip.attributes")
    @ConfigDescription("The list of attributes that will be attached to event. " +
            "Available attributes: country, country_code, region,city, latitude, longitude, timezone")
    public MaxmindGeoIPModuleConfig setAttributes(String attributes) {
        this.attributes = ImmutableList.copyOf(Splitter.on(',').omitEmptyStrings().trimResults().split(attributes));
        return this;
    }

    public boolean getUseExistingFields() {
        return useExistingFields;
    }

    @Config("plugin.geoip.use-existing-fields")
    public MaxmindGeoIPModuleConfig setUseExistingFields(boolean useExistingFields) {
        this.useExistingFields = useExistingFields;
        return this;
    }
}
