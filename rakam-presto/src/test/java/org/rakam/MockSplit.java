package org.rakam;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.SchemaTableName;

import java.util.List;

class MockSplit
        implements ConnectorSplit
{
    private final SchemaTableName tableName;

    public MockSplit(SchemaTableName tableName) {
        this.tableName = tableName;
    }

    public SchemaTableName getTableName() {
        return tableName;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return false;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return com.google.common.collect.ImmutableList.of();
    }

    @Override
    public Object getInfo()
    {
        return null;
    }
}
