package org.rakam.report;

import com.facebook.presto.sql.tree.QualifiedName;
import com.google.auto.service.AutoService;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import org.rakam.MetadataConfig;
import org.rakam.analysis.EventExplorer;
import org.rakam.analysis.FunnelQueryExecutor;
import org.rakam.analysis.JDBCMetastore;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.PrestoMaterializedViewService;
import org.rakam.analysis.PrestoUserService;
import org.rakam.analysis.RetentionQueryExecutor;
import org.rakam.analysis.TimestampToEpochFunction;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.plugin.user.AbstractUserService;
import org.rakam.util.ConditionalModule;
import org.rakam.plugin.ContinuousQueryService;
import org.rakam.plugin.EventExplorerConfig;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.JDBCConfig;
import org.rakam.plugin.MaterializedViewService;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.SystemEvents;
import org.rakam.plugin.TimestampEventMapper;
import org.rakam.plugin.user.UserPluginConfig;
import org.rakam.plugin.user.AbstractPostgresqlUserStorage;
import org.rakam.plugin.user.PrestoExternalUserStorageAdapter;

import javax.inject.Inject;

import static io.airlift.configuration.ConfigBinder.configBinder;

@AutoService(RakamModule.class)
@ConditionalModule(config = "store.adapter", value = "presto")
public class PrestoModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        configBinder(binder).bindConfig(MetadataConfig.class);
        configBinder(binder).bindConfig(PrestoConfig.class);

        binder.bind(QueryExecutor.class).to(PrestoQueryExecutor.class);
        binder.bind(ContinuousQueryService.class).to(PrestoContinuousQueryService.class);
        binder.bind(MaterializedViewService.class).to(PrestoMaterializedViewService.class);
        binder.bind(String.class).annotatedWith(TimestampToEpochFunction.class).toInstance("to_unixtime");

        JDBCConfig config = buildConfigObject(JDBCConfig.class, "presto.metastore.jdbc");
        JDBCPoolDataSource dataSource = JDBCPoolDataSource.getOrCreateDataSource(config);
        binder.bind(JDBCPoolDataSource.class)
                .annotatedWith(Names.named("presto.metastore.jdbc"))
                .toInstance(dataSource);

        binder.bind(Metastore.class).to(JDBCMetastore.class);
        if ("postgresql".equals(getConfig("plugin.user.storage"))) {
            binder.bind(AbstractPostgresqlUserStorage.class).to(PrestoExternalUserStorageAdapter.class)
                    .in(Scopes.SINGLETON);
            binder.bind(AbstractUserService.class).to(PrestoUserService.class)
                    .in(Scopes.SINGLETON);
        }

        if (buildConfigObject(EventExplorerConfig.class).isEventExplorerEnabled()) {
            binder.bind(EventExplorer.class).to(PrestoEventExplorer.class);
        }
        UserPluginConfig userPluginConfig = buildConfigObject(UserPluginConfig.class);

        if(userPluginConfig.getEnableUserMapping()) {
            binder.bind(UserMergeTableHook.class).asEagerSingleton();
        }

        if (userPluginConfig.isFunnelAnalysisEnabled()) {
            binder.bind(FunnelQueryExecutor.class).to(PrestoFunnelQueryExecutor.class);
        }

        if (userPluginConfig.isRetentionAnalysisEnabled()) {
            binder.bind(RetentionQueryExecutor.class).to(PrestoRetentionQueryExecutor.class);
        }

        Multibinder<EventMapper> timeMapper = Multibinder.newSetBinder(binder, EventMapper.class);
        timeMapper.addBinding().to(TimestampEventMapper.class).in(Scopes.SINGLETON);
    }

    @Override
    public String name() {
        return "PrestoDB backend for Rakam";
    }

    @Override
    public String description() {
        return "Rakam backend for high-throughput systems.";
    }

    public static class UserMergeTableHook {
        private final PrestoQueryExecutor executor;

        @Inject
        public UserMergeTableHook(PrestoQueryExecutor executor) {
            this.executor = executor;
        }

        @Subscribe
        public void onCreateProject(SystemEvents.ProjectCreatedEvent event) {
            executor.executeRawStatement(String.format("CREATE TABLE %s(id VARCHAR, _user VARCHAR, created_at DATE, merged_at DATE)",
                    executor.formatTableReference(event.project, QualifiedName.of("_anonymous_id_mapping"))));
        }
    }
}
