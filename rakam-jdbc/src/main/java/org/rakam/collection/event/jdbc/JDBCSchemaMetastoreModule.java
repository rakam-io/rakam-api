package org.rakam.collection.event.jdbc;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.name.Names;
import io.airlift.configuration.ConfigurationModule;
import org.rakam.JDBCConfig;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.plugin.ConditionalModule;
import org.rakam.plugin.RakamModule;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/03/15 03:26.
 */
@AutoService(RakamModule.class)
@ConditionalModule(config="event.schema.store", value="jdbc")
public class JDBCSchemaMetastoreModule extends RakamModule {

    @Override
    protected void setup(Binder binder) {
        ConfigurationModule.bindConfig(binder)
                .annotatedWith(Names.named("event.schema.store.jdbc"))
                .prefixedWith("event.schema.store.jdbc")
                .to(JDBCConfig.class);

        binder.bind(Metastore.class).to(JDBCMetastore.class);
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
