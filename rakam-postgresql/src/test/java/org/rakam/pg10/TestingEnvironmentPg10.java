package org.rakam.pg10;

import io.airlift.testing.postgresql10.TestingPostgreSqlServer;
import org.rakam.config.JDBCConfig;

import java.io.IOException;

public class TestingEnvironmentPg10 {
    private static TestingPostgreSqlServer testingPostgresqlServer;
    private static JDBCConfig postgresqlConfig;

    public TestingEnvironmentPg10() {
        if (testingPostgresqlServer == null) {
            synchronized (TestingEnvironmentPg10.class) {
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
                        throw new RuntimeException("Unable to start PG", e);
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
