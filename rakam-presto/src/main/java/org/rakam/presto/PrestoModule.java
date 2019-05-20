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

        buildConfigObject(JDBCConfig.class, "report.metadata.store.jdbc");

        JDBCPoolDataSource metadataDataSource;
        if ("rakam_raptor".equals(prestoConfig.getColdStorageConnector())) {
            metadataDataSource = bindJDBCConfig(binder, "presto.metastore.jdbc");
        } else {
            metadataDataSource = bindJDBCConfig(binder, "report.metadata.store.jdbc");
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
            } else if (url.startsWith("jdbc:postgresql")) {
                binder.bind(ConfigManager.class).to(PostgresqlConfigManager.class);
            } else {
                throw new IllegalStateException(format("Invalid report metadata database: %s", url));
            }
        }

        Class<? extends PrestoAbstractMetastore> implementation;
        if ("rakam_raptor".equals(prestoConfig.getColdStorageConnector())) {
            implementation = PrestoRakamRaptorMetastore.class;
        } else {
            implementation = PrestoMetastore.class;
        }

        binder.bind(Metastore.class).to(implementation).in(Scopes.SINGLETON);
        binder.bind(PrestoAbstractMetastore.class).to(implementation).in(Scopes.SINGLETON);
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

    private JDBCPoolDataSource bindJDBCConfig(Binder binder, String config) {
        JDBCPoolDataSource dataSource = JDBCPoolDataSource.getOrCreateDataSource(
                buildConfigObject(JDBCConfig.class, config));
        binder.bind(JDBCPoolDataSource.class)
                .annotatedWith(Names.named(config))
                .toInstance(dataSource);
        return dataSource;
    }
}
