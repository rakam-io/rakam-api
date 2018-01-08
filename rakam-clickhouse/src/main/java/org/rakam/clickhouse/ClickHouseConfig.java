package org.rakam.clickhouse;

import io.airlift.configuration.Config;

import java.net.URI;

public class ClickHouseConfig {
    private URI address = URI.create("http://127.0.0.1:8123");
    private String hotStoragePrefix;
    private String coldStoragePrefix;

    public URI getAddress() {
        return address;
    }

    @Config("clickhouse.address")
    public ClickHouseConfig setAddress(URI address) {
        this.address = address;
        return this;
    }

    public String getHotStoragePrefix() {
        return hotStoragePrefix;
    }

    @Config("clickhouse.hot-storage-prefix")
    public ClickHouseConfig setHotStoragePrefix(String hotStoragePrefix) {
        this.hotStoragePrefix = hotStoragePrefix;
        return this;
    }

    public String getColdStoragePrefix() {
        return coldStoragePrefix;
    }

    @Config("clickhouse.cold-storage-prefix")
    public ClickHouseConfig setColdStoragePrefix(String coldStoragePrefix) {
        this.coldStoragePrefix = coldStoragePrefix;
        return this;
    }
}
