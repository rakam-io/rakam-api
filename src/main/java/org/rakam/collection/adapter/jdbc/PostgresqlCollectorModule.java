package org.rakam.collection.adapter.jdbc;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.name.Names;
import io.airlift.configuration.ConfigurationFactory;
import org.rakam.collection.event.metastore.EventSchemaMetastore;
import org.rakam.collection.event.metastore.PostgresqlSchemaMetastore;
import org.rakam.config.ConditionalModule;
import org.rakam.plugin.RakamModule;
import org.rakam.report.metadata.postgresql.PostgresqlConfig;

import static io.airlift.configuration.ConfigurationModule.bindConfig;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/03/15 03:23.
 */
@AutoService(RakamModule.class)
public class PostgresqlCollectorModule extends RakamModule implements ConditionalModule {

    @Override
    protected void setup(Binder binder) {
        bindConfig(binder)
                .annotatedWith(Names.named("event.schema.store.postgresql"))
                .prefixedWith("event.schema.store.postgresql")
                .to(PostgresqlConfig.class);
        binder.bind(EventSchemaMetastore.class).to(PostgresqlSchemaMetastore.class);
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public String description() {
        return null;
    }

    @Override
    public boolean shouldInstall(ConfigurationFactory config) {
        return config.getProperties().get("event.store").toLowerCase().trim().equals("postgresql");
    }
}
