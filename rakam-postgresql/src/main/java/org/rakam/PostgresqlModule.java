package org.rakam;

import com.google.auto.service.AutoService;
import com.google.common.base.Throwables;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import org.rakam.analysis.EventExplorer;
import org.rakam.analysis.FunnelQueryExecutor;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.JDBCQueryMetadata;
import org.rakam.analysis.RetentionQueryExecutor;
import org.rakam.analysis.TimestampToEpochFunction;
import org.rakam.analysis.postgresql.PostgresqlConfig;
import org.rakam.analysis.postgresql.PostgresqlEventStore;
import org.rakam.analysis.postgresql.PostgresqlFunnelQueryExecutor;
import org.rakam.analysis.postgresql.PostgresqlMaterializedViewService;
import org.rakam.analysis.postgresql.PostgresqlMetastore;
import org.rakam.analysis.postgresql.PostgresqlRetentionQueryExecutor;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.user.AbstractUserService;
import org.rakam.util.ConditionalModule;
import org.rakam.plugin.ContinuousQueryService;
import org.rakam.plugin.EventExplorerConfig;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.stream.EventStream;
import org.rakam.plugin.stream.EventStreamConfig;
import org.rakam.plugin.JDBCConfig;
import org.rakam.plugin.MaterializedViewService;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.SystemEvents;
import org.rakam.plugin.user.UserPluginConfig;
import org.rakam.plugin.user.AbstractPostgresqlUserStorage;
import org.rakam.plugin.user.PostgresqlUserService;
import org.rakam.plugin.user.PostgresqlUserStorageAdapter;
import org.rakam.report.QueryExecutor;
import org.rakam.report.postgresql.PostgresqlEventExplorer;
import org.rakam.report.postgresql.PostgresqlPseudoContinuousQueryService;
import org.rakam.report.postgresql.PostgresqlQueryExecutor;

import javax.inject.Inject;
import java.net.URISyntaxException;
import java.util.List;

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
        binder.bind(ContinuousQueryService.class).to(PostgresqlPseudoContinuousQueryService.class).in(Scopes.SINGLETON);
        binder.bind(String.class).annotatedWith(TimestampToEpochFunction.class).toInstance("to_unixtime");

        if (buildConfigObject(EventStreamConfig.class).isEventStreamEnabled()) {
            binder.bind(EventStream.class).to(PostgresqlEventStream.class);
        }

        binder.bind(EventStore.class).to(PostgresqlEventStore.class).in(Scopes.SINGLETON);
        binder.install(getAsyncClientModule(config));

        // use same jdbc pool if report.metadata.store is not set explicitly.
        if(getConfig("report.metadata.store") == null) {
            binder.bind(JDBCPoolDataSource.class)
                    .annotatedWith(Names.named("report.metadata.store.jdbc"))
                    .toInstance(JDBCPoolDataSource.getOrCreateDataSource(config));

            binder.bind(QueryMetadataStore.class).to(JDBCQueryMetadata.class).in(Scopes.SINGLETON);
        }

        if (buildConfigObject(EventExplorerConfig.class).isEventExplorerEnabled()) {
            binder.bind(EventExplorer.class).to(PostgresqlEventExplorer.class);
        }

        if (buildConfigObject(PostgresqlConfig.class).isAutoIndexColumns()) {
            binder.bind(CollectionFieldIndexerListener.class).asEagerSingleton();
        }

        if ("postgresql".equals(getConfig("plugin.user.storage"))) {
            binder.bind(AbstractUserService.class).to(PostgresqlUserService.class)
                    .in(Scopes.SINGLETON);
            binder.bind(AbstractPostgresqlUserStorage.class).to(PostgresqlUserStorageAdapter.class)
                    .in(Scopes.SINGLETON);
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
        return "Postgresql Module";
    }

    @Override
    public String description() {
        return "Postgresql deployment type module";
    }

    private static Module asyncClientModule;

    /*
        This module may be installed more than once, Guice will handle deduplication.
     */
    public synchronized static Module getAsyncClientModule(JDBCConfig config) {
        JDBCConfig asyncClientConfig;
        try {
            final String url = config.getUrl();

            asyncClientConfig = new JDBCConfig()
                    .setPassword(config.getPassword())
                    .setTable(config.getTable())
                    .setMaxConnection(4)
                    .setConnectionMaxLifeTime(0L)
                    .setConnectionIdleTimeout(0L)
                    .setUrl("jdbc:pgsql" + url.substring("jdbc:postgresql".length()))
                    .setUsername(config.getUsername());
        } catch (URISyntaxException e) {
            throw Throwables.propagate(e);
        }

        if(asyncClientModule == null) {
            asyncClientModule = new AbstractConfigurationAwareModule() {
                @Override
                protected void setup(Binder binder) {
                    binder.bind(JDBCPoolDataSource.class)
                            .annotatedWith(Names.named("async-postgresql"))
                            .toProvider(new JDBCPoolDataSourceProvider(asyncClientConfig))
                            .in(Scopes.SINGLETON);
                }
            };
        }
        return asyncClientModule;
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

    private static class CollectionFieldIndexerListener {
        private final PostgresqlQueryExecutor executor;

        @Inject
        public CollectionFieldIndexerListener(PostgresqlQueryExecutor executor) {
            this.executor = executor;
        }

        @Subscribe
        public void onCreateCollection(SystemEvents.CollectionCreatedEvent event) {
            onCreateCollectionFields(event.project, event.collection, event.fields);
        }

        @Subscribe
        public void onCreateCollectionFields(SystemEvents.CollectionFieldCreatedEvent event) {
            onCreateCollectionFields(event.project, event.collection, event.fields);
        }

        public void onCreateCollectionFields(String project, String collection, List<SchemaField> fields) {
            for (SchemaField field : fields) {
                executor.executeRawStatement(String.format("CREATE INDEX %s_%s_%s_auto_index ON %s.\"%s\"(\"%s\")",
                        project, collection, field.getName(),
                        project, collection, field.getName()));
            }
        }
    }
}
