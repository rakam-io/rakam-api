package org.rakam.config;

import io.airlift.configuration.Config;

/**
 * Created by buremba <Burak Emre Kabakcı> on 11/02/15 17:39.
 */
public class MetadataConfig {
    private String store;

    @Config("metadata.store")
    public MetadataConfig setStore(String store)
    {
        this.store = store;
        return this;
    }

    public String getStore() {
        return store;
    }
}
