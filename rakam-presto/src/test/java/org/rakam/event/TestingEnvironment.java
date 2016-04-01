package org.rakam.event;

import com.facebook.presto.rakam.RakamRaptorPlugin;
import com.facebook.presto.rakam.stream.StreamPlugin;
import com.facebook.presto.rakam.stream.metadata.ForMetadata;
import com.facebook.presto.raptor.RaptorPlugin;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.testing.postgresql.TestingPostgreSqlServer;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.config.JDBCConfig;
import org.rakam.presto.analysis.PrestoConfig;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import static com.google.common.base.Throwables.propagate;
import static java.lang.String.format;

public class TestingEnvironment {

    private static PrestoConfig prestoConfig;
    private static TestingPrestoServer testingPrestoServer;
    private static TestingPostgreSqlServer testingPostgresqlServer;
    private static JDBCConfig postgresqlConfig;
    private JDBCPoolDataSource metastore;

    public TestingEnvironment() {
        this(true);
    }

    public TestingEnvironment(boolean installMetadata) {
        try {
            if (testingPrestoServer == null) {
                synchronized (TestingEnvironment.class) {
                    testingPrestoServer = new TestingPrestoServer();

                    String metadataDatabase = Files.createTempDir().getAbsolutePath();
                    RaptorPlugin plugin = new RakamRaptorPlugin() {
                        @Override
                        public void setOptionalConfig(Map<String, String> optionalConfig) {
                            ImmutableMap<String, String> configs = ImmutableMap.of(
                                    "storage.data-directory", Files.createTempDir().getAbsolutePath(),
                                    "metadata.db.type", "h2",
                                    "metadata.db.filename", metadataDatabase);

                            super.setOptionalConfig(ImmutableMap.<String, String>builder().putAll(optionalConfig).putAll(configs).build());
                        }
                    };

                    Module metastoreModule = new Module() {

                        @Override
                        public void configure(Binder binder) {

                        }

                        @Provides
                        @Singleton
                        @ForMetadata
                        public IDBI getDataSource() {
                            return new DBI(format("jdbc:h2:mem:test%s;DB_CLOSE_DELAY=-1;mode=MySQL", System.nanoTime()));
                        }
                    };

                    StreamPlugin streamPlugin = new StreamPlugin("streaming", metastoreModule) {
                        @Override
                        public void setOptionalConfig(Map<String, String> optionalConfig) {
                            super.setOptionalConfig(ImmutableMap.<String, String>builder().putAll(optionalConfig).putAll(ImmutableMap
                                    .of("target.connector_id", "rakam_raptor",
                                        "backup.provider", "file",
                                        "backup.directory", Files.createTempDir().getAbsolutePath(),
                                        "storage.directory", Files.createTempDir().getAbsolutePath(),
                                        "backup.timeout", "1m")).build());
                        }
                    };


                    testingPrestoServer.installPlugin(plugin);
                    testingPrestoServer.installPlugin(streamPlugin);
                    testingPrestoServer.createCatalog("rakam_raptor", "rakam_raptor");
                    testingPrestoServer.createCatalog("streaming", "streaming");

                    prestoConfig = new PrestoConfig()
                            .setAddress(URI.create("http://" + testingPrestoServer.getAddress().toString()))
                            .setStreamingConnector("streaming")
                            .setColdStorageConnector("rakam_raptor");

                    metastore = JDBCPoolDataSource.getOrCreateDataSource(new JDBCConfig().setUrl("jdbc:h2:" + metadataDatabase)
                            .setUsername("sa").setPassword(""));
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

    public JDBCPoolDataSource getPrestoMetastore() {
        return metastore;
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
