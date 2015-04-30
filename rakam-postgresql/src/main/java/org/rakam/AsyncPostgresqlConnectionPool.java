package org.rakam;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.impossibl.postgres.api.jdbc.PGConnection;
import com.impossibl.postgres.jdbc.PGDriver;
import org.apache.commons.dbcp2.BasicDataSource;
import org.rakam.analysis.postgresql.PostgresqlConfig;

import java.sql.SQLException;

/**
 * Created by buremba <Burak Emre Kabakcı> on 28/04/15 03:38.
 */
@Singleton
public class AsyncPostgresqlConnectionPool {
    BasicDataSource connectionPool;

    @Inject
    public AsyncPostgresqlConnectionPool(PostgresqlConfig config) {
        connectionPool = new BasicDataSource();
        connectionPool.setUsername(config.getUsername());
        connectionPool.setPassword(config.getPassword());
        connectionPool.setDriverClassName(PGDriver.class.getName());
        connectionPool.setUrl("jdbc:pgsql://" + config.getHost() + ':' + config.getPort() + "/" + config.getDatabase());
        connectionPool.setInitialSize(1);
    }

    public PGConnection getConnection() throws SQLException {
        return (PGConnection) connectionPool.getConnection();
    }
}
