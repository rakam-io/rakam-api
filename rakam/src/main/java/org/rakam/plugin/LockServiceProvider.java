package org.rakam.plugin;

import com.google.common.base.Throwables;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import com.mysql.jdbc.MySQLConnection;
import org.postgresql.PGConnection;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.util.lock.LockService;
import org.rakam.util.lock.MysqlLockService;
import org.rakam.util.lock.PostgresqlLockService;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.SQLException;

public class LockServiceProvider
        implements Provider<LockService> {
    private final JDBCPoolDataSource dataSource;

    @Inject
    public LockServiceProvider(@Named("report.metadata.store.jdbc") JDBCPoolDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public LockService get() {
        // TODO: get rid of this hack
        Connection connection = null;
        try {
            connection = dataSource.getConnection(true);
            if (connection instanceof MySQLConnection) {
                return new MysqlLockService(dataSource);
            } else if (connection instanceof PGConnection) {
                return new PostgresqlLockService(dataSource);
            } else {
                throw new RuntimeException("Lock service requires Postgresql or Mysql as dependency.");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    throw Throwables.propagate(e);
                }
            }
        }
    }
}
