package org.rakam.presto;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import org.rakam.analysis.ApiKeyService;
import org.rakam.analysis.ConfigManager;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.config.JDBCConfig;
import org.rakam.config.MetadataConfig;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.TimestampEventMapper;
import org.rakam.postgresql.PostgresqlConfigManager;
import org.rakam.postgresql.analysis.JDBCApiKeyService;
import org.rakam.presto.analysis.*;
import org.rakam.util.ConditionalModule;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.lang.String.format;

@AutoService(RakamModule.class)
@ConditionalModule(config = "store.adapter", value = "presto")
public class PrestoModule
        extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        configBinder(binder).bindConfig(MetadataConfig.class);
        configBinder(binder).bindConfig(PrestoConfig.class);
        PrestoConfig prestoConfig = buildConfigObject(PrestoConfig.class);

        buildConfigObject(JDBCConfig.class, "metadata.store.jdbc");

        JDBCPoolDataSource metadataDataSource = bindJDBCConfig(binder, "metadata.store.jdbc");

        binder.bind(ApiKeyService.class).toInstance(new JDBCApiKeyService(metadataDataSource));
        binder.bind(ConfigManager.class).to(MysqlConfigManager.class);
        binder.bind(JDBCPoolDataSource.class)
                .annotatedWith(Names.named("metadata.store.jdbc"))
                .toInstance(metadataDataSource);

        binder.bind(Metastore.class).to(PrestoRakamRaptorMetastore.class).in(Scopes.SINGLETON);
    }

    @Override
    public String name() {
        return "PrestoDB backend for Rakam";
    }

    @Override
    public String description() {
        return "Rakam backend for high-throughput systems.";
    }

    private JDBCPoolDataSource bindJDBCConfig(Binder binder, String config) {
        JDBCPoolDataSource dataSource = JDBCPoolDataSource.getOrCreateDataSource(
                buildConfigObject(JDBCConfig.class, config));
        binder.bind(JDBCPoolDataSource.class)
                .annotatedWith(Names.named(config))
                .toInstance(dataSource);
        return dataSource;
    }
}
