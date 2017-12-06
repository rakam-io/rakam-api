package org.rakam.postgresql;

import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.postgresql.analysis.JDBCApiKeyService;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class PostgresqlApiKeyService extends JDBCApiKeyService {
    @Inject
    public PostgresqlApiKeyService(JDBCPoolDataSource connectionPool) {
        super(connectionPool);
    }

    @Override
    public void setup() {
        try (Connection connection = connectionPool.getConnection()) {
            Statement statement = connection.createStatement();
            statement.execute("CREATE TABLE IF NOT EXISTS api_key (" +
                    "  id SERIAL NOT NULL,\n" +
                    "  project VARCHAR(255) NOT NULL,\n" +
                    "  read_key VARCHAR(255) NOT NULL,\n" +
                    "  write_key VARCHAR(255) NOT NULL,\n" +
                    "  master_key VARCHAR(255) NOT NULL,\n" +
                    "  created_at TIMESTAMP default current_timestamp NOT NULL," +
                    "PRIMARY KEY (id)\n" +
                    "  )");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
