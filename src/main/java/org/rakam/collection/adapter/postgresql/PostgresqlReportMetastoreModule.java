package org.rakam.collection.adapter.postgresql;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.name.Names;
import io.airlift.configuration.ConfigurationFactory;
import org.rakam.config.ConditionalModule;
import org.rakam.plugin.RakamModule;
import org.rakam.report.metadata.ReportMetadataStore;
import org.rakam.report.metadata.postgresql.PostgresqlConfig;
import org.rakam.report.metadata.postgresql.PostgresqlReportMetadata;

import static io.airlift.configuration.ConfigurationModule.bindConfig;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/03/15 03:26.
 */
@AutoService(RakamModule.class)
public class PostgresqlReportMetastoreModule extends RakamModule implements ConditionalModule {
    @Override
    public boolean shouldInstall(ConfigurationFactory config) {
        return config.getProperties().get("report.metadata.store").toLowerCase().trim().equals("postgresql");
    }

    @Override
    protected void setup(Binder binder) {
        bindConfig(binder)
                .annotatedWith(Names.named("report.metadata.store.postgresql"))
                .prefixedWith("report.metadata.store.postgresql")
                .to(PostgresqlConfig.class);
        binder.bind(ReportMetadataStore.class).to(PostgresqlReportMetadata.class);
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
