package org.rakam;

import com.google.auto.service.AutoService;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Binder;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import org.rakam.analysis.EventExplorer;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.JDBCQueryMetadata;
import org.rakam.analysis.postgresql.PostgresqlContinuousQueryService;
import org.rakam.analysis.postgresql.PostgresqlEventStore;
import org.rakam.analysis.postgresql.PostgresqlMaterializedViewService;
import org.rakam.analysis.postgresql.PostgresqlMetastore;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.ConditionalModule;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.ContinuousQueryService;
import org.rakam.plugin.EventExplorerConfig;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.EventStream;
import org.rakam.plugin.EventStreamConfig;
import org.rakam.plugin.JDBCConfig;
import org.rakam.plugin.MaterializedViewService;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.SystemEvents;
import org.rakam.report.QueryExecutor;
import org.rakam.report.postgresql.PostgresqlEventExplorer;
import org.rakam.report.postgresql.PostgresqlQueryExecutor;

import javax.inject.Inject;
import java.net.URISyntaxException;

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
        binder.bind(ContinuousQueryService.class).to(PostgresqlContinuousQueryService.class).in(Scopes.SINGLETON);

        JDBCConfig asyncClientConfig;
        try {
            final String url = config.getUrl();

            asyncClientConfig = new JDBCConfig()
                    .setMaxConnection(config.getMaxConnection())
                    .setPassword(config.getPassword())
                    .setTable(config.getTable())
                    .setMaxConnection(5)
                    .setUrl("jdbc:pgsql" + url.substring("jdbc:postgresql".length()))
                    .setUsername(config.getUsername());
        } catch (URISyntaxException e) {
            throw Throwables.propagate(e);
        }

        binder.bind(JDBCPoolDataSource.class)
                .annotatedWith(Names.named("async-postgresql"))
                .toProvider(new JDBCPoolDataSourceProvider(asyncClientConfig))
                .in(Scopes.SINGLETON);

        if (buildConfigObject(EventStreamConfig.class).isEventStreamEnabled()) {
            binder.bind(EventStream.class).to(PostgresqlEventStream.class);
        }

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

            binder.bind(EventExplorerListener.class).asEagerSingleton();
        }


    }

    @Override
    public String name() {
        return "Postgresql Module";
    }

    @Override
    public String description() {
        return "Postgresql deployment type module";
    }

    public static class EventExplorerListener {
        private static final String QUERY = "select _time/3600 as time, count(*) as total from stream group by 1";
        private final PostgresqlContinuousQueryService continuousQueryService;

        @Inject
        public EventExplorerListener(PostgresqlContinuousQueryService continuousQueryService) {
            this.continuousQueryService = continuousQueryService;
        }

        @Subscribe
        public void onCreateCollection(SystemEvents.CollectionCreatedEvent event) {
            ContinuousQuery report = new ContinuousQuery(event.project, "Total count of "+event.collection,
                    "_total_" + event.collection,
                    QUERY,
                    ImmutableList.of(event.collection),
                    ImmutableList.of(), ImmutableMap.of());
            continuousQueryService.create(report);
        }
    }

    private static class JDBCPoolDataSourceProvider implements Provider<JDBCPoolDataSource> {
        private final JDBCConfig asyncClientConfig;

        public JDBCPoolDataSourceProvider(JDBCConfig asyncClientConfig) {
            this.asyncClientConfig = asyncClientConfig;
        }

        @Override
        public JDBCPoolDataSource get() {
            return JDBCPoolDataSource.getOrCreateDataSource(asyncClientConfig);
        }
    }
}
