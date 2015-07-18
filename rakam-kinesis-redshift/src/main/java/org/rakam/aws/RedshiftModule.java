package org.rakam.aws;

import com.google.inject.Binder;
import org.rakam.analysis.stream.RakamContinuousQueryService;
import org.rakam.analysis.RedshiftMaterializedViewService;
import org.rakam.analysis.RedshiftQueryExecutor;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.plugin.ConditionalModule;
import org.rakam.plugin.ContinuousQueryService;
import org.rakam.plugin.MaterializedViewService;
import org.rakam.plugin.RakamModule;
import org.rakam.report.QueryExecutor;
import org.rakam.analysis.RedshiftConfig;

import static io.airlift.configuration.ConfigurationModule.bindConfig;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/07/15 19:12.
 */
@ConditionalModule(config="store.adapter", value="aws")
public class RedshiftModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        bindConfig(binder).to(RedshiftConfig.class);
        binder.bind(Metastore.class).to(RedshiftMetastore.class);

        binder.bind(QueryExecutor.class).to(RedshiftQueryExecutor.class);
        binder.bind(ContinuousQueryService.class).to(RakamContinuousQueryService.class);
        binder.bind(MaterializedViewService.class).to(RedshiftMaterializedViewService.class);
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
