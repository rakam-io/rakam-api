package org.rakam.report;

import com.facebook.presto.hive.$internal.com.google.common.collect.ImmutableList;
import com.facebook.presto.hive.$internal.com.google.common.collect.ImmutableMap;
import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.multibindings.OptionalBinder;
import com.google.inject.name.Names;
import org.rakam.MetadataConfig;
import org.rakam.analysis.EventExplorer;
import org.rakam.analysis.FunnelQueryExecutor;
import org.rakam.analysis.JDBCMetastore;
import org.rakam.analysis.PrestoAbstractUserService;
import org.rakam.analysis.PrestoMaterializedViewService;
import org.rakam.analysis.RetentionQueryExecutor;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.plugin.AbstractUserService;
import org.rakam.plugin.ConditionalModule;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.ContinuousQueryService;
import org.rakam.plugin.JDBCConfig;
import org.rakam.plugin.MaterializedViewService;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.SystemEventListener;

import static io.airlift.configuration.ConfigurationModule.bindConfig;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/04/15 06:31.
 */
@AutoService(RakamModule.class)
@ConditionalModule(config="store.adapter", value="presto")
public class PrestoModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        bindConfig(binder).to(MetadataConfig.class);
        binder.bind(QueryExecutor.class).to(PrestoQueryExecutor.class);
        binder.bind(ContinuousQueryService.class).to(PrestoContinuousQueryService.class);
        binder.bind(MaterializedViewService.class).to(PrestoMaterializedViewService.class);


        bindConfig(binder)
                .annotatedWith(Names.named("presto.metastore.jdbc"))
                .prefixedWith("presto.metastore.jdbc")
                .to(JDBCConfig.class);

        binder.bind(Metastore.class).to(JDBCMetastore.class);
        if (getConfig("plugin.user.storage") != null) {
            OptionalBinder.newOptionalBinder(binder, AbstractUserService.class)
                    .setBinding().to(PrestoAbstractUserService.class);
        }

        if ("true".equals( getConfig("event-explorer.enabled"))) {
            binder.bind(EventExplorer.class).to(PrestoEventExplorer.class);

            Multibinder<SystemEventListener> events = Multibinder.newSetBinder(binder, SystemEventListener.class);
            events.addBinding().to(EventExplorerListener.class).in(Scopes.SINGLETON);
        }

        if ("true".equals(getConfig("user.funnel-analysis.enabled"))) {
            binder.bind(FunnelQueryExecutor.class).to(PrestoFunnelQueryExecutor.class);
        }

        if ("true".equals(getConfig("user.retention-analysis.enabled"))) {
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
                    ImmutableMap.of());
            continuousQueryService.create(report);
        }
    }
}
