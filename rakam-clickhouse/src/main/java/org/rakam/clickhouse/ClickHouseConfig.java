package org.rakam.clickhouse;

import java.net.URI;
import io.airlift.configuration.Config;

public class ClickHouseConfig
{
    private URI address = URI.create("http://127.0.0.1:8123");
    private String hotStoragePrefix;
    private String coldStoragePrefix;

    @Config("clickhouse.address")
    public ClickHouseConfig setAddress(URI address)
    {
        this.address = address;
        return this;
    }

    public URI getAddress()
    {
        return address;
    }

    public String getHotStoragePrefix()
    {
        return hotStoragePrefix;
    }

    public String getColdStoragePrefix()
    {
        return coldStoragePrefix;
    }

    @Config("clickhouse.hot-storage-prefix")
    public ClickHouseConfig setHotStoragePrefix(String hotStoragePrefix)
    {
        this.hotStoragePrefix = hotStoragePrefix;
        return this;
    }

    @Config("clickhouse.cold-storage-prefix")
    public ClickHouseConfig setColdStoragePrefix(String hotStoragePrefix)
    {
        this.coldStoragePrefix = coldStoragePrefix;
        return this;
    }
}
