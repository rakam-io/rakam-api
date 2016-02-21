package org.rakam.event;

import com.facebook.presto.raptor.RaptorPlugin;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.airlift.testing.postgresql.TestingPostgreSqlServer;
import org.rakam.config.JDBCConfig;
import org.rakam.presto.analysis.PrestoConfig;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import static com.google.common.base.Throwables.propagate;

public class TestingEnvironment {

    private static PrestoConfig prestoConfig;
    private static TestingPrestoServer testingPrestoServer;
    private static TestingPostgreSqlServer testingPostgresqlServer;
    private static JDBCConfig postgresqlConfig;

    public TestingEnvironment() {
        this(true);
    }

    public TestingEnvironment(boolean installMetadata) {
        try {
            if (testingPrestoServer == null) {
                synchronized (TestingEnvironment.class) {
                    testingPrestoServer = new TestingPrestoServer();

                    RaptorPlugin plugin = new RaptorPlugin() {
                        @Override
                        public void setOptionalConfig(Map<String, String> optionalConfig) {
                            ImmutableMap<String, String> configs = ImmutableMap.of(
                                    "storage.data-directory", Files.createTempDir().getAbsolutePath(),
                                    "metadata.db.type", "h2",
                                    "metadata.db.filename", Files.createTempDir().getAbsolutePath());

                            super.setOptionalConfig(ImmutableMap.<String, String>builder().putAll(optionalConfig).putAll(configs).build());
                        }
                    };

                    testingPrestoServer.installPlugin(plugin);
                    testingPrestoServer.createCatalog("raptor", "raptor");

                    prestoConfig = new PrestoConfig()
                            .setAddress(URI.create("http://" + testingPrestoServer.getAddress().toString()))
                            .setStreamingConnector("raptor")
                            .setColdStorageConnector("raptor");
                }
            }
            if (installMetadata) {
                if (testingPostgresqlServer == null) {
                    synchronized (TestingEnvironment.class) {
                        testingPostgresqlServer = new TestingPostgreSqlServer("testuser", "testdb");

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
                    }
                }

                postgresqlConfig = new JDBCConfig()
                        .setUrl(testingPostgresqlServer.getJdbcUrl())
                        .setUsername(testingPostgresqlServer.getUser());
            }
        } catch (Exception e) {
            throw propagate(e);
        }
    }

    public JDBCConfig getPostgresqlConfig() {
        if (postgresqlConfig == null) {
            throw new UnsupportedOperationException();
        }
        return postgresqlConfig;
    }

    public PrestoConfig getPrestoConfig() {
        return prestoConfig;
    }

    public void close() throws Exception {
        testingPrestoServer.close();
        if (testingPostgresqlServer != null) {
            testingPostgresqlServer.close();
        }
    }
}
