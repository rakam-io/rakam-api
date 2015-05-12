package org.rakam;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.name.Names;
import org.rakam.analysis.postgresql.PostgresqlConfig;
import org.rakam.analysis.postgresql.PostgresqlContinuousQueryService;
import org.rakam.analysis.postgresql.PostgresqlEventStore;
import org.rakam.analysis.postgresql.PostgresqlMaterializedViewService;
import org.rakam.analysis.postgresql.PostgresqlMetastore;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.AbstractUserService;
import org.rakam.plugin.ConditionalModule;
import org.rakam.plugin.ContinuousQueryService;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.EventStream;
import org.rakam.plugin.JDBCConfig;
import org.rakam.plugin.MaterializedViewService;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.UserStorage;
import org.rakam.plugin.user.PostgresqlUserService;
import org.rakam.plugin.user.PostgresqlUserStorageAdapter;
import org.rakam.report.QueryExecutor;
import org.rakam.analysis.JDBCQueryMetadata;
import org.rakam.report.postgresql.PostgresqlQueryExecutor;

import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 24/03/15 03:23.
 */
@AutoService(RakamModule.class)
@ConditionalModule(config="store.adapter", value="postgresql")
public class PostgresqlModule extends RakamModule {

    @Override
    protected void setup(Binder binder) {
        PostgresqlConfig config = buildConfigObject(PostgresqlConfig.class);
        binder.bind(Metastore.class).to(PostgresqlMetastore.class);
        // TODO: implement postgresql specific materialized view service
        binder.bind(MaterializedViewService.class).to(PostgresqlMaterializedViewService.class);
        binder.bind(QueryExecutor.class).to(PostgresqlQueryExecutor.class);
        binder.bind(ContinuousQueryService.class).to(PostgresqlContinuousQueryService.class);
        binder.bind(EventStream.class).to(PostgresqlEventStream.class);
        binder.bind(AbstractUserService.class).to(PostgresqlUserService.class);
        binder.bind(EventStore.class).to(PostgresqlEventStore.class);

        MetadataConfig metadataConfig = buildConfigObject(MetadataConfig.class);
        if(metadataConfig.getUserStore() == null || metadataConfig.getUserStore().equals("postgresql")) {
            binder.bind(UserStorage.class).to(PostgresqlUserStorageAdapter.class);
        }

        JDBCConfig jdbcConfig = new JDBCConfig();
        jdbcConfig.setUrl(format("jdbc:postgresql://%s:%d/%s", config.getHost(), config.getPort(), config.getDatabase()));
        jdbcConfig.setTable("rakam_metadata");
        jdbcConfig.setPassword(config.getPassword());
        jdbcConfig.setUsername(config.getUsername());

        binder.bind(JDBCConfig.class)
                .annotatedWith(Names.named("report.metadata.store.jdbc"))
                .toInstance(jdbcConfig);

        JDBCQueryMetadata jdbcQueryMetadata = new JDBCQueryMetadata(jdbcConfig);
        binder.bind(QueryMetadataStore.class).toInstance(jdbcQueryMetadata);
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
