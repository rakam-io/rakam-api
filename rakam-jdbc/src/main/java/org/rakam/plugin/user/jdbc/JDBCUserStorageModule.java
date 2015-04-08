package org.rakam.plugin.user.jdbc;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.name.Names;
import io.airlift.configuration.ConfigurationFactory;
import org.rakam.JDBCConfig;
import org.rakam.plugin.ConditionalModule;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.UserStorage;

import static io.airlift.configuration.ConfigurationModule.bindConfig;

/**
 * Created by buremba <Burak Emre Kabakcı> on 17/03/15 18:08.
 */
@AutoService(RakamModule.class)
public class JDBCUserStorageModule extends RakamModule implements ConditionalModule {
    @Override
    protected void setup(Binder binder) {
        bindConfig(binder).to(JDBCUserStorageConfig.class);

        bindConfig(binder)
                .annotatedWith(Names.named("plugin.user.storage.jdbc"))
                .prefixedWith("plugin.user.storage.jdbc")
                .to(JDBCConfig.class);

        binder.bind(UserStorage.class).to(JDBCUserStorageAdapter.class);
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public String description() {
        return null;
    }

    @Override
    public boolean shouldInstall(ConfigurationFactory config) {
        return config.getProperties().get("plugin.user.storage").equals("jdbc");
    }
}
