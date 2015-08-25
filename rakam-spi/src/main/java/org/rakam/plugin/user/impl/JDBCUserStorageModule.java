package org.rakam.plugin.user.impl;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.name.Names;
import org.rakam.plugin.ConditionalModule;
import org.rakam.plugin.JDBCConfig;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.UserStorage;

import static io.airlift.configuration.ConfigurationModule.bindConfig;

/**
 * Created by buremba <Burak Emre Kabakcı> on 17/03/15 18:08.
 */
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
        return "JDBC backend for ";
    }

    @Override
    public String description() {
        return null;
    }
}
