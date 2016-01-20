package org.rakam.event;

import com.facebook.presto.raptor.RaptorPlugin;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.airlift.testing.postgresql.TestingPostgreSqlServer;
import org.rakam.plugin.JDBCConfig;
import org.rakam.report.PrestoConfig;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

public class TestingEnvironment {

    protected PrestoConfig prestoConfig;
    private TestingPrestoServer testingPrestoServer;
    private TestingPostgreSqlServer testingPostgresqlServer;
    protected JDBCConfig postgresqlConfig;

    public TestingEnvironment() throws Exception {
        testingPrestoServer = new TestingPrestoServer();
        testingPostgresqlServer = new TestingPostgreSqlServer("testuser", "testdb");

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
        postgresqlConfig = new JDBCConfig()
//                .setUrl("jdbc:postgresql://127.0.0.1:5432/testng")
//                .setUsername("buremba");
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

    public JDBCConfig getPostgresqlConfig() {
        return postgresqlConfig;
    }

    public PrestoConfig getPrestoConfig() {
        return prestoConfig;
    }

    public void close() throws Exception {
        testingPrestoServer.close();
        testingPostgresqlServer.close();
    }
}
