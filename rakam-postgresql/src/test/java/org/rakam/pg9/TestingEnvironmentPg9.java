package org.rakam.pg9;

import com.google.common.base.Throwables;
import io.airlift.testing.postgresql.TestingPostgreSqlServer;
import org.rakam.config.JDBCConfig;

import java.io.IOException;

public class TestingEnvironmentPg9 {
    private static TestingPostgreSqlServer testingPostgresqlServer;
    private static JDBCConfig postgresqlConfig;

    public TestingEnvironmentPg9() {
        if (testingPostgresqlServer == null) {
            synchronized (TestingEnvironmentPg9.class) {
                if (testingPostgresqlServer == null) {
                    try {
                        testingPostgresqlServer = new TestingPostgreSqlServer("testuser", "testdb");
                        testingPostgresqlServer.execute("ALTER USER testuser WITH SUPERUSER");
                        postgresqlConfig = new JDBCConfig()
                                .setUrl(testingPostgresqlServer.getJdbcUrl())
                                .setUsername(testingPostgresqlServer.getUser());
                        Runtime.getRuntime().addShutdownHook(
                                new Thread(
                                        () -> {
                                            try {
                                                testingPostgresqlServer.close();
                                            } catch (IOException e) {
                                                e.printStackTrace();
                                            }
                                        }
                                )
                        );
                    } catch (Exception e) {
                        throw Throwables.propagate(e);
                    }
                }
            }
        }
    }

    public JDBCConfig getPostgresqlConfig() {
        if (postgresqlConfig == null) {
            throw new UnsupportedOperationException();
        }
        return postgresqlConfig;
    }
}
