package org.rakam.collection.adapter.jdbc;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.name.Names;
import io.airlift.configuration.ConfigurationFactory;
import org.rakam.config.ConditionalModule;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.user.mailbox.jdbc.JDBCUserMailboxConfig;
import org.rakam.report.metadata.ReportMetadataStore;
import org.rakam.report.metadata.postgresql.JDBCReportMetadata;

import static io.airlift.configuration.ConfigurationModule.bindConfig;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/03/15 03:26.
 */
@AutoService(RakamModule.class)
public class JDBCReportMetastoreModule extends RakamModule implements ConditionalModule {
    @Override
    public boolean shouldInstall(ConfigurationFactory config) {
        return config.getProperties().get("report.metadata.store").toLowerCase().trim().equals("jdbc");
    }

    @Override
    protected void setup(Binder binder) {
        bindConfig(binder)
                .annotatedWith(Names.named("report.metadata.store.jdbc"))
                .prefixedWith("report.metadata.store.jdbc")
                .to(JDBCUserMailboxConfig.class);

        binder.bind(ReportMetadataStore.class).to(JDBCReportMetadata.class);
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
