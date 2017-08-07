package org.rakam.plugin;

import com.google.auto.service.AutoService;
import com.google.common.base.Throwables;
import com.google.inject.Binder;
import com.google.inject.Provider;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.mysql.jdbc.MySQLConnection;
import io.airlift.configuration.Config;
import org.postgresql.PGConnection;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.util.lock.LockService;
import org.rakam.plugin.tasks.ScheduledTaskHttpService;
import org.rakam.server.http.HttpService;
import org.rakam.util.ConditionalModule;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.SQLException;

import static io.airlift.configuration.ConfigBinder.configBinder;

@AutoService(RakamModule.class)
@ConditionalModule(config = "tasks.enable", value = "true")
public class ScheduledTaskModule
        extends RakamModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(TaskConfig.class);
        Multibinder<HttpService> httpServices = Multibinder.newSetBinder(binder, HttpService.class);
        httpServices.addBinding().to(ScheduledTaskHttpService.class);

        binder.bind(LockService.class).toProvider(LockServiceProvider.class);
        binder.bind(String.class).annotatedWith(Names.named("timestamp_function"))
                .toProvider(DatabaseFunction.class);
    }

    @Override
    public String name()
    {
        return null;
    }

    @Override
    public String description()
    {
        return null;
    }

    public static class DatabaseFunction
            implements Provider<String>
    {

        private final JDBCPoolDataSource dataSource;

        @Inject
        public DatabaseFunction(@Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource)
        {
            this.dataSource = dataSource;
        }

        @Override
        public String get()
        {
            // TODO: get rid of this hack
            Connection connection = null;
            try {
                connection = dataSource.getConnection(true);
                if (connection instanceof MySQLConnection) {
                    return "unix_timestamp(now())";
                }
                else if (connection instanceof PGConnection) {
                    return "to_unixtime(cast(now() as timestamp))";
                }
                else {
                    throw new RuntimeException("Lock service requires Postgresql or Mysql as dependency.");
                }
            }
            catch (SQLException e) {
                throw Throwables.propagate(e);
            }
            finally {
                if (connection != null) {
                    try {
                        connection.close();
                    }
                    catch (SQLException e) {
                        throw Throwables.propagate(e);
                    }
                }
            }
        }
    }

    public static class TaskConfig {
        public boolean enabled;

        public boolean getEnabled()
        {
            return enabled;
        }

        @Config("tasks.enable")
        public TaskConfig setEnabled(boolean enabled)
        {
            this.enabled = enabled;
            return this;
        }
    }
}
