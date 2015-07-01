package org.rakam.report;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.multibindings.OptionalBinder;
import com.google.inject.name.Names;
import io.airlift.configuration.ConfigurationModule;
import org.rakam.MetadataConfig;
import org.rakam.analysis.JDBCMetastore;
import org.rakam.analysis.PrestoAbstractUserService;
import org.rakam.analysis.PrestoMaterializedViewService;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.plugin.AbstractUserService;
import org.rakam.plugin.ConditionalModule;
import org.rakam.plugin.ContinuousQueryService;
import org.rakam.plugin.JDBCConfig;
import org.rakam.plugin.MaterializedViewService;
import org.rakam.plugin.RakamModule;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/04/15 06:31.
 */
@AutoService(RakamModule.class)
@ConditionalModule(config="store.adapter", value="presto")
public class PrestoModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        MetadataConfig metadataConfig = buildConfigObject(MetadataConfig.class);
        binder.bind(QueryExecutor.class).to(PrestoQueryExecutor.class);
        binder.bind(ContinuousQueryService.class).to(PrestoContinuousQueryService.class);
        binder.bind(MaterializedViewService.class).to(PrestoMaterializedViewService.class);

        ConfigurationModule.bindConfig(binder)
                .annotatedWith(Names.named("presto.metastore.jdbc"))
                .prefixedWith("presto.metastore.jdbc")
                .to(JDBCConfig.class);

        binder.bind(Metastore.class).to(JDBCMetastore.class);
        if(metadataConfig.getUserStore() != null) {
            OptionalBinder.newOptionalBinder(binder, AbstractUserService.class)
                    .setBinding().to(PrestoAbstractUserService.class);
        }

//        binder.bind(ReportMetadataStore.class).to(PrestoReportMetadata.class);

//        bindConfig(binder).annotatedWith(Names.named("report.metadata.store.presto"))
//                .prefixedWith("report.metadata.store.presto").to(PrestoReportMetadataConfig.class);
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
