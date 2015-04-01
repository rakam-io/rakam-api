package org.rakam.plugin.user.mailbox.jdbc;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.name.Names;
import io.airlift.configuration.ConfigurationFactory;
import org.rakam.config.ConditionalModule;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.user.mailbox.UserMailboxStorage;

import static io.airlift.configuration.ConfigurationModule.bindConfig;

/**
 * Created by buremba <Burak Emre Kabakcı> on 30/03/15 15:55.
 */
@AutoService(RakamModule.class)
public class JDBCUserMailboxModule extends RakamModule implements ConditionalModule {
    @Override
    public boolean shouldInstall(ConfigurationFactory config) {
        return config.getProperties().get("plugin.user.mailbox.persistence").equals("jdbc");
    }

    @Override
    protected void setup(Binder binder) {
        bindConfig(binder)
                .annotatedWith(Names.named("plugin.user.mailbox.persistence.jdbc"))
                .prefixedWith("plugin.user.mailbox.persistence.jdbc")
                .to(JDBCUserMailboxConfig.class);
        binder.bind(UserMailboxStorage.class).to(JDBCUserMailboxStorage.class);
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
