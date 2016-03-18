package org.rakam;

import com.google.common.base.Throwables;
import io.airlift.testing.postgresql.TestingPostgreSqlServer;
import org.rakam.config.JDBCConfig;

import java.io.IOException;

public class TestingEnvironment {
    private static TestingPostgreSqlServer testingPostgresqlServer;
    private static JDBCConfig postgresqlConfig;

    public TestingEnvironment() {
        if(testingPostgresqlServer == null) {
            synchronized (TestingEnvironment.class) {
                if(testingPostgresqlServer == null) {
                    try {
                        testingPostgresqlServer = new TestingPostgreSqlServer("testuser", "testdb");
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
