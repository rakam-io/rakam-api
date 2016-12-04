package org.rakam.postgresql.plugin.user;

import com.google.inject.Binder;
import com.google.inject.name.Names;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.config.JDBCConfig;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.user.mailbox.UserMailboxStorage;
import org.rakam.postgresql.plugin.user.mailbox.PostgresqlUserMailboxStorage;

import static org.rakam.postgresql.PostgresqlModule.getAsyncClientModule;

//@AutoService(RakamModule.class)
//@ConditionalModule(config="plugin.user.mailbox.adapter", value="postgresql")
public class PostgresqlUserMailboxModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        JDBCConfig config = buildConfigObject(JDBCConfig.class, "store.adapter.postgresql");

        binder.bind(JDBCPoolDataSource.class)
                .annotatedWith(Names.named("store.adapter.postgresql"))
                .toInstance(JDBCPoolDataSource.getOrCreateDataSource(config));

        binder.install(getAsyncClientModule(config));
        binder.bind(UserMailboxStorage.class).to(PostgresqlUserMailboxStorage.class);
    }

    @Override
    public String name() {
        return "Postgresql User mailbox module";
    }

    @Override
    public String description() {
        return "Real-time mailbox module that allows your users to get mail notifications in real-time. Can also be used as chat application.";
    }
}
