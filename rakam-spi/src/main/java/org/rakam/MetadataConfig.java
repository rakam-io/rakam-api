package org.rakam;

import io.airlift.configuration.Config;

public class MetadataConfig {
    private String metastore;
    private String eventStore;
    private String reportMetastore;

    @Config("event.schema.store")
    public MetadataConfig setMetastore(String store) {
        this.metastore = store;
        return this;
    }

    @Config("report.metadata.store")
    public MetadataConfig setReportMetastore(String store) {
        this.reportMetastore = store;
        return this;
    }

    public String getReportMetastore() {
        return reportMetastore;
    }

    public String getMetastore() {
        return metastore;
    }

    @Config("event.store")
    public MetadataConfig setEventStore(String eventStore) {
        this.eventStore = eventStore;
        return this;
    }

    public String getEventStore() {
        return eventStore;
    }
}
