package org.rakam;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;

class TestingHandleResolver implements ConnectorHandleResolver {
    @Override
    public boolean canHandle(ConnectorTableHandle tableHandle) {
        return tableHandle instanceof TestingMetadata.InMemoryTableHandle;
    }

    @Override
    public boolean canHandle(ColumnHandle columnHandle) {
        return columnHandle instanceof TestingMetadata.InMemoryColumnHandle;
    }

    @Override
    public boolean canHandle(ConnectorSplit split) {
        return split instanceof MockSplit;
    }

    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass() {
        return TestingMetadata.InMemoryTableHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass() {
        return TestingMetadata.InMemoryColumnHandle.class;
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass() {
        return MockSplit.class;
    }
}
