package org.rakam.analysis;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.name.Names;
import io.airlift.configuration.ConfigurationModule;
import org.rakam.plugin.JDBCConfig;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.ConditionalModule;
import org.rakam.plugin.RakamModule;


@AutoService(RakamModule.class)
@ConditionalModule(config="report.metadata.store", value="jdbc")
public class JDBCReportMetastoreModule extends RakamModule {

    @Override
    protected void setup(Binder binder) {
        ConfigurationModule.bindConfig(binder)
                .annotatedWith(Names.named("report.metadata.store.jdbc"))
                .prefixedWith("report.metadata.store.jdbc")
                .to(JDBCConfig.class);

        binder.bind(QueryMetadataStore.class).to(JDBCQueryMetadata.class);
    }

    @Override
    public String name() {
        return "JDBC report metadata store";
    }

    @Override
    public String description() {
        return "Stores report metadata (materialized, continuous queries) in RDBMS databases";
    }
}
