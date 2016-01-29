package org.rakam.analysis;

import org.rakam.collection.event.metastore.Metastore;
import org.rakam.plugin.EventStore;

public class TestPostgresqlRetentionQueryExecutor extends TestRetentionQueryExecutor {
    @Override
    public EventStore getEventStore() {
        return null;
    }

    @Override
    public Metastore getMetastore() {
        return null;
    }

    @Override
    public RetentionQueryExecutor getRetentionQueryExecutor() {
        return null;
    }
}
