package org.rakam.analysis;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.name.Names;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.util.ConditionalModule;
import org.rakam.plugin.JDBCConfig;
import org.rakam.plugin.RakamModule;


@AutoService(RakamModule.class)
@ConditionalModule(config="report.metadata.store", value="jdbc")
public class JDBCReportMetastoreModule extends RakamModule {

    @Override
    protected void setup(Binder binder) {
        JDBCConfig config = buildConfigObject(JDBCConfig.class,
                "report.metadata.store.jdbc");

        binder.bind(JDBCPoolDataSource.class)
                .annotatedWith(Names.named("report.metadata.store.jdbc"))
                .toInstance(JDBCPoolDataSource.getOrCreateDataSource(config));

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
