package org.rakam.presto;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import org.rakam.analysis.ApiKeyService;
import org.rakam.analysis.ConfigManager;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.config.JDBCConfig;
import org.rakam.config.MetadataConfig;
import org.rakam.plugin.RakamModule;
import org.rakam.postgresql.analysis.JDBCApiKeyService;
import org.rakam.presto.analysis.*;
import org.rakam.util.ConditionalModule;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.lang.String.format;

@AutoService(RakamModule.class)
@ConditionalModule(config = "metadata.store.jdbc.url")
public class MetadataModule
        extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        JDBCConfig jdbc = buildConfigObject(JDBCConfig.class, "metadata.store.jdbc");
        if(!jdbc.getUrl().startsWith("jdbc:mysql")) {
            throw new IllegalArgumentException("We only support as metadata store at the moment");
        }
        JDBCPoolDataSource metadataDataSource = bindJDBCConfig(binder, "metadata.store.jdbc");
        binder.bind(ApiKeyService.class).toInstance(new JDBCApiKeyService(metadataDataSource));
        binder.bind(JDBCPoolDataSource.class).annotatedWith(Names.named("metadata.store.jdbc")).toInstance(metadataDataSource);


        // we only support mysql at the moment
        binder.bind(ConfigManager.class).to(MysqlConfigManager.class);
        binder.bind(Metastore.class).to(MysqlExplicitMetastore.class).in(Scopes.SINGLETON);
    }

    @Override
    public String name() {
        return "Metadata store for Rakam";
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
