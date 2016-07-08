package org.rakam.clickhouse;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import org.rakam.analysis.ApiKeyService;
import org.rakam.analysis.ConfigManager;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.analysis.EscapeIdentifier;
import org.rakam.analysis.EventExplorer;
import org.rakam.analysis.FunnelQueryExecutor;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.analysis.RealtimeService.RealtimeAggregations;
import org.rakam.analysis.RetentionQueryExecutor;
import org.rakam.analysis.TimestampToEpochFunction;
import org.rakam.analysis.metadata.JDBCQueryMetadata;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.analysis.metadata.QueryMetadataStore;
import org.rakam.aws.dynamodb.metastore.DynamodbMetastore;
import org.rakam.clickhouse.analysis.ClickHouseEventExplorer;
import org.rakam.clickhouse.analysis.ClickHouseFunnelQueryExecutor;
import org.rakam.clickhouse.analysis.ClickHouseMetastore;
import org.rakam.clickhouse.analysis.ClickHouseRetentionQueryExecutor;
import org.rakam.clickhouse.collection.ClickHouseEventStore;
import org.rakam.config.MetadataConfig;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.TimestampEventMapper;
import org.rakam.plugin.user.AbstractUserService;
import org.rakam.plugin.user.UserPluginConfig;
import org.rakam.plugin.user.UserStorage;
import org.rakam.report.QueryExecutor;
import org.rakam.report.eventexplorer.EventExplorerConfig;
import org.rakam.report.realtime.AggregationType;
import org.rakam.util.ConditionalModule;

import java.util.List;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.rakam.report.realtime.AggregationType.APPROXIMATE_UNIQUE;
import static org.rakam.report.realtime.AggregationType.COUNT;
import static org.rakam.report.realtime.AggregationType.COUNT_UNIQUE;
import static org.rakam.report.realtime.AggregationType.MAXIMUM;
import static org.rakam.report.realtime.AggregationType.MINIMUM;
import static org.rakam.report.realtime.AggregationType.SUM;

@AutoService(RakamModule.class)
@ConditionalModule(config = "store.adapter", value = "clickhouse")
public class ClickHouseModule
        extends RakamModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(MetadataConfig.class);
        configBinder(binder).bindConfig(ClickHouseConfig.class);

        binder.bind(char.class).annotatedWith(EscapeIdentifier.class).toInstance('`');

        binder.bind(QueryExecutor.class).to(ClickHouseQueryExecutor.class);
        binder.bind(EventStore.class).to(ClickHouseEventStore.class);
        binder.bind(ContinuousQueryService.class).to(ClickHouseContinuousQueryService.class);
        binder.bind(MaterializedViewService.class).to(ClickHouseMaterializedViewService.class);
        binder.bind(String.class).annotatedWith(TimestampToEpochFunction.class)
                .toInstance("toUnixTimestamp");

        binder.bind(new TypeLiteral<List<AggregationType>>() {})
                .annotatedWith(RealtimeAggregations.class)
                .toInstance(ImmutableList.of(
                        COUNT,
                        SUM,
                        MINIMUM,
                        MAXIMUM,
                        APPROXIMATE_UNIQUE,
                        COUNT_UNIQUE));

        binder.bind(AbstractUserService.class).to(ClickHouseUserService.class)
                .in(Scopes.SINGLETON);

        if (buildConfigObject(EventExplorerConfig.class).isEventExplorerEnabled()) {
            binder.bind(EventExplorer.class).to(ClickHouseEventExplorer.class);
        }
        UserPluginConfig userPluginConfig = buildConfigObject(UserPluginConfig.class);

        if (userPluginConfig.getEnableUserMapping()) {
            throw new IllegalStateException("Clickhouse module doesn't support user mapping.");
        }

        if (userPluginConfig.isFunnelAnalysisEnabled()) {
            binder.bind(FunnelQueryExecutor.class).to(ClickHouseFunnelQueryExecutor.class);
        }

        if (userPluginConfig.isRetentionAnalysisEnabled()) {
            binder.bind(RetentionQueryExecutor.class).to(ClickHouseRetentionQueryExecutor.class);
        }

        Multibinder<EventMapper> timeMapper = Multibinder.newSetBinder(binder, EventMapper.class);
        timeMapper.addBinding().to(TimestampEventMapper.class).in(Scopes.SINGLETON);
    }

    @Override
    public String name()
    {
        return "ClickHouse backend for Rakam";
    }

    @Override
    public String description()
    {
        return "Rakam backend for big-data.";
    }
}
