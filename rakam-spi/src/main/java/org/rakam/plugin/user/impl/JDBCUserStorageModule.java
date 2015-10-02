package org.rakam.plugin.user.impl;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.name.Names;
import org.rakam.plugin.ConditionalModule;
import org.rakam.plugin.JDBCConfig;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.UserStorage;

import static io.airlift.configuration.ConfigurationModule.bindConfig;


@AutoService(RakamModule.class)
@ConditionalModule(config="plugin.user.storage", value="jdbc")
public class JDBCUserStorageModule extends RakamModule {
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
        return "JDBC Backend for User Data";
    }

    @Override
    public String description() {
        return "Allows you to plug your existing RDBMS databases to Rakam.";
    }
}
