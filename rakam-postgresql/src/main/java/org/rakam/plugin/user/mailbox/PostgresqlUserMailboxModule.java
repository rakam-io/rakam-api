package org.rakam.plugin.user.mailbox;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import io.airlift.configuration.ConfigurationFactory;
import org.rakam.analysis.postgresql.PostgresqlConfig;
import org.rakam.plugin.ConditionalModule;
import org.rakam.plugin.RakamModule;

import static io.airlift.configuration.ConfigurationModule.bindConfig;

/**
 * Created by buremba <Burak Emre Kabakcı> on 30/03/15 15:55.
 */
@AutoService(RakamModule.class)
public class PostgresqlUserMailboxModule extends RakamModule implements ConditionalModule {
    @Override
    public boolean shouldInstall(ConfigurationFactory config) {
        String s = config.getProperties().get("plugin.user.mailbox.adapter");
        if(s.equals("postgresql")) {
            config.consumeProperty("plugin.user.mailbox.adapter");
            return true;
        }
        return false;
    }

    @Override
    protected void setup(Binder binder) {
        bindConfig(binder).to(PostgresqlConfig.class);
        binder.bind(UserMailboxStorage.class).to(PostgresqlUserMailboxStorage.class);
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
