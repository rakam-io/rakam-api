package org.rakam;

import com.google.inject.Singleton;
import com.impossibl.postgres.api.jdbc.PGConnection;
import com.impossibl.postgres.jdbc.PGConnectionPoolDataSource;
import org.rakam.analysis.postgresql.PostgresqlConfig;

import javax.inject.Inject;

import java.sql.SQLException;

@Singleton
public class AsyncPostgresqlConnectionPool {
    PGConnectionPoolDataSource connectionPool;

    @Inject
    public AsyncPostgresqlConnectionPool(PostgresqlConfig config) {
        connectionPool = new PGConnectionPoolDataSource();
        connectionPool.setUser(config.getUsername());
        connectionPool.setPassword(config.getPassword());
        connectionPool.setDatabase(config.getDatabase());
        connectionPool.setHousekeeper(false);
    }

    public PGConnection getConnection() throws SQLException {
        return (PGConnection) connectionPool.getPooledConnection().getConnection();
    }
}
