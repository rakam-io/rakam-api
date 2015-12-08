package org.rakam.plugin.user;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.name.Names;
import org.rakam.PostgresqlModule;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.plugin.ConditionalModule;
import org.rakam.plugin.JDBCConfig;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.user.mailbox.PostgresqlUserMailboxStorage;
import org.rakam.plugin.user.mailbox.UserMailboxStorage;

@AutoService(RakamModule.class)
@ConditionalModule(config="plugin.user.mailbox.adapter", value="postgresql")
public class PostgresqlUserMailboxModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        JDBCConfig config = buildConfigObject(JDBCConfig.class, "store.adapter.postgresql");

        binder.bind(JDBCPoolDataSource.class)
                .annotatedWith(Names.named("store.adapter.postgresql"))
                .toInstance(JDBCPoolDataSource.getOrCreateDataSource(config));

        binder.install(PostgresqlModule.getAsyncClientModule(config));
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
