import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.commons.dbcp2.BasicDataSource;
import redshift.RedshiftConfig;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by buremba <Burak Emre Kabakcı> on 03/07/15 04:36.
 */
@Singleton
public class RedshiftPoolDataSource {

    private final BasicDataSource connectionPool;

    @Inject
    public RedshiftPoolDataSource(RedshiftConfig config) {
        connectionPool = new BasicDataSource();
        connectionPool.setUsername(config.getUsername());
        connectionPool.setPassword(config.getPassword());
        connectionPool.setDriverClassName(org.postgresql.Driver.class.getName());
        connectionPool.setUrl("jdbc:postgresql://" + config.getHost() + ':' + config.getPort() + "/" + config.getDatabase());
        connectionPool.setMaxTotal(config.getMaxConnection());
        connectionPool.setPoolPreparedStatements(true);
    }

    public Connection getConnection() throws SQLException {
        return connectionPool.getConnection();
    }
}

