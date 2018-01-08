package org.rakam.config;

import io.airlift.configuration.Config;
import io.swagger.models.ExternalDocs;

public class MetadataConfig {
    public final static ExternalDocs centralDocs = new ExternalDocs().url("https://rakam.io/doc").description("Rakam Documentation");

    private String metastore;
    private String eventStore;
    private String reportMetastore;
    private boolean enableDynamic = true;

    public boolean getEnableDynamic() {
        return enableDynamic;
    }

    @Config("event.schema.enable-dynamic")
    public MetadataConfig setEnableDynamic(boolean enableDynamic) {
        this.enableDynamic = enableDynamic;
        return this;
    }

    public String getReportMetastore() {
        return reportMetastore;
    }

    @Config("report.metadata.store")
    public MetadataConfig setReportMetastore(String store) {
        this.reportMetastore = store;
        return this;
    }

    public String getMetastore() {
        return metastore;
    }

    @Config("event.schema.store")
    public MetadataConfig setMetastore(String store) {
        this.metastore = store;
        return this;
    }

    public String getEventStore() {
        return eventStore;
    }

    @Config("event.store")
    public MetadataConfig setEventStore(String eventStore) {
        this.eventStore = eventStore;
        return this;
    }
}
