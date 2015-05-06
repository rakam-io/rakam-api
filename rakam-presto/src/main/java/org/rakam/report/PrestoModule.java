package org.rakam.report;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import org.rakam.analysis.PrestoAbstractUserService;
import org.rakam.analysis.PrestoMaterializedViewService;
import org.rakam.plugin.AbstractUserService;
import org.rakam.plugin.ConditionalModule;
import org.rakam.plugin.ContinuousQueryService;
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
        binder.bind(QueryExecutor.class).to(PrestoQueryExecutor.class);
        binder.bind(ContinuousQueryService.class).to(PrestoContinuousQueryService.class);
        binder.bind(MaterializedViewService.class).to(PrestoMaterializedViewService.class);
        binder.bind(AbstractUserService.class).to(PrestoAbstractUserService.class);

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
