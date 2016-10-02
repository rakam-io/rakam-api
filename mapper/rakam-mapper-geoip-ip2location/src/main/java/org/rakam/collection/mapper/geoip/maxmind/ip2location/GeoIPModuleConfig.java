package org.rakam.collection.mapper.geoip.maxmind.ip2location;

import io.airlift.configuration.Config;

import java.util.List;

public class GeoIPModuleConfig
{
    private List<String> attributes;
    private String databaseUrl = null;
    private int dbId = 3;

    @Config("plugin.geoip.database.url")
    public GeoIPModuleConfig setDatabaseUrl(String url)
    {
        this.databaseUrl = url;
        return this;
    }

    public String getDatabaseUrl()
    {
        return databaseUrl;
    }

    @Config("plugin.geoip.database.dbid")
    public GeoIPModuleConfig setDbId(int dbId)
    {
        this.dbId = dbId;
        return this;
    }

    public int getDbId()
    {
        return dbId;
    }
}
