package org.rakam.collection.mapper.geoip;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;

import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 12/02/15 21:09.
 */
public class GeoIPModuleConfig {
    private String database;
    private List<String> attributes;

    @Config("module.geoip.database")
    public GeoIPModuleConfig setDatabase(String type)
    {
        this.database = type;
        return this;
    }
    @Config("module.geoip.attributes")
    public GeoIPModuleConfig setAttributes(String attributes)
    {
        this.attributes = ImmutableList.copyOf(Splitter.on(',').omitEmptyStrings().trimResults().split(attributes));
        return this;
    }

    public String getDatabase() {
        return database;
    }

    public List<String> getAttributes() {
        return attributes;
    }
}
