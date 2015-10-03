package org.rakam.report;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.multibindings.OptionalBinder;
import com.google.inject.name.Names;
import org.rakam.MetadataConfig;
import org.rakam.analysis.*;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.plugin.*;

import javax.inject.Inject;

import static io.airlift.configuration.ConfigurationModule.bindConfig;

@AutoService(RakamModule.class)
@ConditionalModule(config="store.adapter", value="presto")
public class PrestoModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        bindConfig(binder).to(MetadataConfig.class);
        bindConfig(binder).to(PrestoConfig.class);

        binder.bind(QueryExecutor.class).to(PrestoQueryExecutor.class);
        binder.bind(ContinuousQueryService.class).to(PrestoContinuousQueryService.class);
        binder.bind(MaterializedViewService.class).to(PrestoMaterializedViewService.class);

        JDBCConfig config = buildConfigObject(JDBCConfig.class, "presto.metastore.jdbc");

        JDBCPoolDataSource dataSource = new JDBCPoolDataSource(config);
        binder.bind(JDBCPoolDataSource.class)
                .annotatedWith(Names.named("presto.metastore.jdbc"))
                .toInstance(dataSource);

        binder.bind(Metastore.class).to(JDBCMetastore.class);
        if (getConfig("plugin.user.storage") != null) {
            OptionalBinder.newOptionalBinder(binder, AbstractUserService.class)
                    .setBinding().to(PrestoAbstractUserService.class);
        }

        if (buildConfigObject(EventExplorerConfig.class).isEventExplorerEnabled()) {
            binder.bind(EventExplorer.class).to(PrestoEventExplorer.class);

            Multibinder<SystemEventListener> events = Multibinder.newSetBinder(binder, SystemEventListener.class);
            events.addBinding().to(EventExplorerListener.class).in(Scopes.SINGLETON);
        }

        UserPluginConfig userPluginConfig = buildConfigObject(UserPluginConfig.class);

        if (userPluginConfig.isFunnelAnalysisEnabled()) {
            binder.bind(FunnelQueryExecutor.class).to(PrestoFunnelQueryExecutor.class);
        }

        if (userPluginConfig.isRetentionAnalysisEnabled()) {
            binder.bind(RetentionQueryExecutor.class).to(PrestoRetentionQueryExecutor.class);
        }

    }

    @Override
    public String name() {
        return "PrestoDB backend for Rakam";
    }

    @Override
    public String description() {
        return "Rakam backend for high-throughput systems.";
    }

    public static class EventExplorerListener implements SystemEventListener {
        private static final String QUERY = "select time/3600 as time, count(*) as total from stream group by 1";
        private final PrestoContinuousQueryService continuousQueryService;

        @Inject
        public EventExplorerListener(PrestoContinuousQueryService continuousQueryService) {
            this.continuousQueryService = continuousQueryService;
        }

        @Override
        public void onCreateCollection(String project, String collection) {
            ContinuousQuery report = new ContinuousQuery(project, "Total count of "+collection,
                    "_total_" + collection,
                    QUERY,
                    ImmutableList.of(collection),
                    ImmutableList.of(), ImmutableMap.of());
            continuousQueryService.create(report);
        }
    }
}
