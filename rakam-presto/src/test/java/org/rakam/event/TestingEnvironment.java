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

    private final PrestoConfig prestoConfig;
    private final TestingPrestoServer testingPrestoServer;
    private TestingPostgreSqlServer testingPostgresqlServer;
    private JDBCConfig postgresqlConfig;

    public TestingEnvironment() {
        this(true);
    }

    public TestingEnvironment(boolean installMetadata) {
        try {
            testingPrestoServer = new TestingPrestoServer();

            if (installMetadata) {
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
            }
        } catch (Exception e) {
            throw propagate(e);
        }

        ImmutableMap<String, String> configs = ImmutableMap.of(
                "storage.data-directory", Files.createTempDir().getAbsolutePath(),
                "metadata.db.type", "h2",
                "metadata.db.filename", Files.createTempDir().getAbsolutePath());

        RaptorPlugin plugin = new RaptorPlugin() {
            @Override
            public void setOptionalConfig(Map<String, String> optionalConfig) {
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

    public JDBCConfig getPostgresqlConfig() {
        if(postgresqlConfig == null) {
            throw new UnsupportedOperationException();
        }
        return postgresqlConfig;
    }

    public PrestoConfig getPrestoConfig() {
        return prestoConfig;
    }

    public void close() throws Exception {
        testingPrestoServer.close();
        if(testingPostgresqlServer != null) {
            testingPostgresqlServer.close();
        }
    }
}
