package org.rakam.collection;

import com.google.inject.Binder;
import com.google.inject.name.Names;
import org.rakam.collection.event.EventStore;
import org.rakam.collection.event.datastore.KafkaEventStore;
import org.rakam.collection.event.metastore.EventSchemaMetastore;
import org.rakam.collection.event.metastore.KumeSchemaMetastore;
import org.rakam.collection.event.metastore.PostgresqlSchemaMetastore;
import org.rakam.config.KafkaConfig;
import org.rakam.config.MetadataConfig;
import org.rakam.plugin.RakamModule;
import org.rakam.report.PrestoConfig;
import org.rakam.report.metadata.ReportMetadataStore;
import org.rakam.report.metadata.postgresql.PostgresqlConfig;
import org.rakam.report.metadata.postgresql.PostgresqlReportMetadata;

import static io.airlift.configuration.ConfigurationModule.bindConfig;

/**
 * Created by buremba <Burak Emre Kabakcı> on 11/02/15 17:26.
 */
public class CollectionModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        MetadataConfig config = buildConfigObject(MetadataConfig.class);

        switch (config.getEventStore().toLowerCase().trim()) {
            case "kafka":
                bindConfig(binder)
                        .annotatedWith(Names.named("event.store.kafka"))
                        .prefixedWith("event.store.kafka")
                        .to(KafkaConfig.class);
                binder.bind(EventStore.class).to(KafkaEventStore.class);
                break;
        }

        switch (config.getMetastore().toLowerCase().trim()) {
            case "kafka":
                binder.bind(EventSchemaMetastore.class).to(KumeSchemaMetastore.class);
                break;
            case "postgresql":
                bindConfig(binder)
                        .annotatedWith(Names.named("event.schema.store.postgresql"))
                        .prefixedWith("event.schema.store.postgresql")
                        .to(PostgresqlConfig.class);
                binder.bind(EventSchemaMetastore.class).to(PostgresqlSchemaMetastore.class);
                break;
        }

        switch (config.getReportMetastore().toLowerCase().trim()) {
            case "postgresql":
                bindConfig(binder)
                        .annotatedWith(Names.named("report.metadata.store.postgresql"))
                        .prefixedWith("report.metadata.store.postgresql")
                        .to(PostgresqlConfig.class);
                binder.bind(ReportMetadataStore.class).to(PostgresqlReportMetadata.class);
                break;
        }
        bindConfig(binder).to(PrestoConfig.class);
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public String description() {
        return null;
    }
}
