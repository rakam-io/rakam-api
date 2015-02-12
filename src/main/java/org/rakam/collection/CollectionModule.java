package org.rakam.collection;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import org.rakam.collection.event.metastore.EventSchemaMetastore;
import org.rakam.collection.event.metastore.KumeSchemaMetastore;
import org.rakam.collection.event.metastore.PostgresqlSchemaMetastore;
import org.rakam.config.MetadataConfig;
import org.rakam.report.metadata.postgresql.PostgresqlMetadataConfig;

/**
 * Created by buremba <Burak Emre Kabakcı> on 11/02/15 17:26.
 */
public class CollectionModule extends AbstractConfigurationAwareModule {
    @Override
    protected void setup(Binder binder) {
        MetadataConfig config = buildConfigObject(MetadataConfig.class);

        switch (config.getStore().toLowerCase().trim()) {
            case "kume":
                binder.bind(EventSchemaMetastore.class).to(KumeSchemaMetastore.class).in(Scopes.SINGLETON);
            case "postgresql":
                PostgresqlSchemaMetastore metadata = new PostgresqlSchemaMetastore(buildConfigObject(PostgresqlMetadataConfig.class));
                binder.bind(EventSchemaMetastore.class).toInstance(metadata);
        }
    }
}
