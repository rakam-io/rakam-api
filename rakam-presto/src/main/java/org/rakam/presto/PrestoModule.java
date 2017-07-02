package org.rakam.presto;

import com.facebook.presto.sql.tree.QualifiedName;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Binder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.multibindings.OptionalBinder;
import com.google.inject.name.Names;
import org.rakam.analysis.ApiKeyService;
import org.rakam.analysis.ConfigManager;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.analysis.EscapeIdentifier;
import org.rakam.analysis.EventExplorer;
import org.rakam.analysis.FunnelQueryExecutor;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.analysis.RetentionQueryExecutor;
import org.rakam.analysis.TimestampToEpochFunction;
import org.rakam.analysis.metadata.JDBCQueryMetadata;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.analysis.metadata.QueryMetadataStore;
import org.rakam.aws.kinesis.ForStreamer;
import org.rakam.config.JDBCConfig;
import org.rakam.config.MetadataConfig;
import org.rakam.config.ProjectConfig;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.SystemEvents.ProjectCreatedEvent;
import org.rakam.plugin.TimestampEventMapper;
import org.rakam.plugin.stream.EventStream;
import org.rakam.plugin.stream.EventStreamConfig;
import org.rakam.plugin.user.AbstractUserService;
import org.rakam.plugin.user.UserPluginConfig;
import org.rakam.postgresql.PostgresqlConfigManager;
import org.rakam.postgresql.analysis.JDBCApiKeyService;
import org.rakam.postgresql.plugin.user.AbstractPostgresqlUserStorage;
import org.rakam.presto.analysis.MysqlConfigManager;
import org.rakam.presto.analysis.PrestoAbstractMetastore;
import org.rakam.presto.analysis.PrestoConfig;
import org.rakam.presto.analysis.PrestoContinuousQueryService;
import org.rakam.presto.analysis.PrestoEventExplorer;
import org.rakam.presto.analysis.PrestoEventStream;
import org.rakam.presto.analysis.PrestoFunnelQueryExecutor;
import org.rakam.presto.analysis.PrestoMaterializedViewService;
import org.rakam.presto.analysis.PrestoMetastore;
import org.rakam.presto.analysis.PrestoQueryExecutor;
import org.rakam.presto.analysis.PrestoRakamRaptorMetastore;
import org.rakam.presto.analysis.PrestoRetentionQueryExecutor;
import org.rakam.presto.analysis.PrestoUserService;
import org.rakam.presto.plugin.user.PrestoExternalUserStorageAdapter;
import org.rakam.report.QueryExecutor;
import org.rakam.report.eventexplorer.EventExplorerConfig;
import org.rakam.report.realtime.AggregationType;
import org.rakam.report.realtime.RealTimeConfig;
import org.rakam.util.ConditionalModule;
import org.rakam.util.RakamException;

import javax.inject.Inject;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.List;
import java.util.Optional;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static org.rakam.presto.analysis.PrestoUserService.ANONYMOUS_ID_MAPPING;
import static org.rakam.report.realtime.AggregationType.APPROXIMATE_UNIQUE;
import static org.rakam.report.realtime.AggregationType.COUNT;
import static org.rakam.report.realtime.AggregationType.MAXIMUM;
import static org.rakam.report.realtime.AggregationType.MINIMUM;
import static org.rakam.report.realtime.AggregationType.SUM;
import static org.rakam.util.ValidationUtil.checkCollection;

@AutoService(RakamModule.class)
@ConditionalModule(config = "store.adapter", value = "presto")
public class PrestoModule
        extends RakamModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(MetadataConfig.class);
        configBinder(binder).bindConfig(PrestoConfig.class);
        PrestoConfig prestoConfig = buildConfigObject(PrestoConfig.class);
        OptionalBinder<JDBCConfig> userConfig = OptionalBinder.newOptionalBinder(binder, Key.get(JDBCConfig.class, UserConfig.class));

        binder.bind(QueryExecutor.class).to(PrestoQueryExecutor.class);
        binder.bind(char.class).annotatedWith(EscapeIdentifier.class).toInstance('"');
        binder.bind(MaterializedViewService.class).to(PrestoMaterializedViewService.class);
        binder.bind(String.class).annotatedWith(TimestampToEpochFunction.class).toInstance("to_unixtime");

        buildConfigObject(JDBCConfig.class, "report.metadata.store.jdbc");

        JDBCPoolDataSource metadataDataSource;
        if ("rakam_raptor".equals(prestoConfig.getColdStorageConnector())) {
            if (prestoConfig.getEnableStreaming()) {
                binder.bind(ContinuousQueryService.class).to(PrestoContinuousQueryService.class);
            }
            else {
                binder.bind(ContinuousQueryService.class).to(PrestoPseudoContinuousQueryService.class);
            }

            metadataDataSource = bindJDBCConfig(binder, "presto.metastore.jdbc");

            if (buildConfigObject(EventStreamConfig.class).getEventStreamEnabled()) {
                httpClientBinder(binder).bindHttpClient("streamer", ForStreamer.class);
                binder.bind(EventStream.class).to(PrestoEventStream.class).in(Scopes.SINGLETON);
            }
        }
        else {
            metadataDataSource = bindJDBCConfig(binder, "report.metadata.store.jdbc");
            binder.bind(ContinuousQueryService.class).to(PrestoPseudoContinuousQueryService.class);
        }

        binder.bind(ApiKeyService.class).toInstance(new JDBCApiKeyService(metadataDataSource));

        // use same jdbc pool if report.metadata.store is not set explicitly.
        if (getConfig("report.metadata.store") == null) {
            binder.bind(JDBCPoolDataSource.class)
                    .annotatedWith(Names.named("report.metadata.store.jdbc"))
                    .toInstance(metadataDataSource);

            String url = metadataDataSource.getConfig().getUrl();
            if (url.startsWith("jdbc:mysql")) {
                binder.bind(ConfigManager.class).to(MysqlConfigManager.class);
            }
            else if (url.startsWith("jdbc:postgresql")) {
                binder.bind(ConfigManager.class).to(PostgresqlConfigManager.class);
            }
            else {
                throw new IllegalStateException(format("Invalid report metadata database: %s", url));
            }

            binder.bind(QueryMetadataStore.class).to(JDBCQueryMetadata.class)
                    .in(Scopes.SINGLETON);
        }

        Class<? extends PrestoAbstractMetastore> implementation;
        if ("rakam_raptor".equals(prestoConfig.getColdStorageConnector())) {
            implementation = PrestoRakamRaptorMetastore.class;
        }
        else {
            implementation = PrestoMetastore.class;
        }

        binder.bind(Metastore.class).to(implementation).in(Scopes.SINGLETON);
        binder.bind(PrestoAbstractMetastore.class).to(implementation).in(Scopes.SINGLETON);

        if ("postgresql".equals(getConfig("plugin.user.storage"))) {
            binder.bind(AbstractPostgresqlUserStorage.class).to(PrestoExternalUserStorageAdapter.class)
                    .in(Scopes.SINGLETON);
            binder.bind(AbstractUserService.class).to(PrestoUserService.class)
                    .in(Scopes.SINGLETON);

            userConfig.setBinding().toInstance(buildConfigObject(JDBCConfig.class, "store.adapter.postgresql"));
        }

        EventExplorerConfig eventExplorerConfig = buildConfigObject(EventExplorerConfig.class);
        if (eventExplorerConfig.isEventExplorerEnabled()) {
            binder.bind(EventExplorer.class).to(PrestoEventExplorer.class);
        }
        UserPluginConfig userPluginConfig = buildConfigObject(UserPluginConfig.class);

        if (userPluginConfig.getEnableUserMapping()) {
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
    public String name()
    {
        return "PrestoDB backend for Rakam";
    }

    @Override
    public String description()
    {
        return "Rakam backend for high-throughput systems.";
    }

    private JDBCPoolDataSource bindJDBCConfig(Binder binder, String config)
    {
        JDBCPoolDataSource dataSource = JDBCPoolDataSource.getOrCreateDataSource(
                buildConfigObject(JDBCConfig.class, config));
        binder.bind(JDBCPoolDataSource.class)
                .annotatedWith(Names.named(config))
                .toInstance(dataSource);
        return dataSource;
    }

    public static class UserMergeTableHook
    {
        private final PrestoQueryExecutor executor;
        private final ProjectConfig projectConfig;

        @Inject
        public UserMergeTableHook(ProjectConfig projectConfig, PrestoQueryExecutor executor)
        {
            this.projectConfig = projectConfig;
            this.executor = executor;
        }

        @Subscribe
        public void onCreateProject(ProjectCreatedEvent event)
        {
            executor.executeRawStatement(format("CREATE TABLE %s(id VARCHAR, %s VARCHAR, " +
                            "created_at TIMESTAMP, merged_at TIMESTAMP)",
                    executor.formatTableReference(event.project, QualifiedName.of(ANONYMOUS_ID_MAPPING), Optional.empty(), ImmutableMap.of(), "collection"),
                    checkCollection(projectConfig.getUserColumn())));
        }
    }

    @BindingAnnotation
    @Target({FIELD, PARAMETER, METHOD})
    @Retention(RUNTIME)
    public @interface UserConfig {}
}
