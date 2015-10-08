package org.rakam;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.multibindings.OptionalBinder;
import com.google.inject.name.Names;
import org.rakam.analysis.*;
import org.rakam.analysis.postgresql.*;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.*;
import org.rakam.plugin.user.PostgresqlUserService;
import org.rakam.report.QueryExecutor;
import org.rakam.report.postgresql.PostgresqlEventExplorer;
import org.rakam.report.postgresql.PostgresqlQueryExecutor;

import javax.inject.Inject;

@AutoService(RakamModule.class)
@ConditionalModule(config="store.adapter", value="postgresql")
public class PostgresqlModule extends RakamModule {

    @Override
    protected void setup(Binder binder) {
        JDBCConfig config = buildConfigObject(JDBCConfig.class, "store.adapter.postgresql");

        binder.bind(JDBCPoolDataSource.class)
                .annotatedWith(Names.named("store.adapter.postgresql"))
                .toInstance(JDBCPoolDataSource.getOrCreateDataSource(config));

        binder.bind(Metastore.class).to(PostgresqlMetastore.class).in(Scopes.SINGLETON);
        // TODO: implement postgresql specific materialized view service
        binder.bind(MaterializedViewService.class).to(PostgresqlMaterializedViewService.class).in(Scopes.SINGLETON);
        binder.bind(QueryExecutor.class).to(PostgresqlQueryExecutor.class).in(Scopes.SINGLETON);
        OptionalBinder.newOptionalBinder(binder, ContinuousQueryService.class)
                .setBinding()
                .to(PostgresqlContinuousQueryService.class).in(Scopes.SINGLETON);
        binder.bind(EventStream.class).to(PostgresqlEventStream.class);
        binder.bind(AbstractUserService.class).to(PostgresqlUserService.class).in(Scopes.SINGLETON);
        binder.bind(EventStore.class).to(PostgresqlEventStore.class).in(Scopes.SINGLETON);

        // use same jdbc pool if report.metadata.store is not set explicitly.
        if(getConfig("report.metadata.store") == null) {
            binder.bind(JDBCPoolDataSource.class)
                    .annotatedWith(Names.named("report.metadata.store.jdbc"))
                    .toInstance(JDBCPoolDataSource.getOrCreateDataSource(config));

            binder.bind(QueryMetadataStore.class).to(JDBCQueryMetadata.class).in(Scopes.SINGLETON);
        }

        if (buildConfigObject(EventExplorerConfig.class).isEventExplorerEnabled()) {
            binder.bind(EventExplorer.class).to(PostgresqlEventExplorer.class);

            Multibinder<SystemEventListener> events = Multibinder.newSetBinder(binder, SystemEventListener.class);
            events.addBinding().to(EventExplorerListener.class).in(Scopes.SINGLETON);
        }

        UserPluginConfig userPluginConfig = buildConfigObject(UserPluginConfig.class);

        if (userPluginConfig.isFunnelAnalysisEnabled()) {
            binder.bind(FunnelQueryExecutor.class).to(PostgresqlFunnelQueryExecutor.class);
        }

        if (userPluginConfig.isRetentionAnalysisEnabled()) {
            binder.bind(RetentionQueryExecutor.class).to(PostgresqlRetentionQueryExecutor.class);
        }
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public String description() {
        return null;
    }

    public static class EventExplorerListener implements SystemEventListener {
        private static final String QUERY = "select time/3600 as time, count(*) as total from stream group by 1";
        private final PostgresqlContinuousQueryService continuousQueryService;

        @Inject
        public EventExplorerListener(PostgresqlContinuousQueryService continuousQueryService) {
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
