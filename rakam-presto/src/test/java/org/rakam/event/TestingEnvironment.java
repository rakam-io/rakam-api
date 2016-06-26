package org.rakam.event;

import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
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

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Throwables.propagate;
import static com.google.common.collect.ImmutableList.of;
import static java.lang.ProcessBuilder.Redirect.INHERIT;
import static java.lang.String.format;
import static java.lang.System.getProperty;

public class TestingEnvironment {

    private static PrestoConfig prestoConfig;
    private static TestingPrestoServer testingPrestoServer;
    private static TestingPostgreSqlServer testingPostgresqlServer;
    private static JDBCConfig postgresqlConfig;
    private static int kinesisPort;
    private static JDBCPoolDataSource metastore;
    private static Process kinesisProcess;
    DynamoDBProxyServer dynamoDBServer;
    private Process dynamodbServer;

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
                            ImmutableMap<String, String> configs = ImmutableMap.<String, String>builder()
                                    .put("storage.data-directory", Files.createTempDir().getAbsolutePath())
                                    .put("metadata.db.type", "h2")
                                    .put("metadata.db.connections.wait", "10s")
                                    .put("metadata.db.connections.max", "500")
                                    .put("metadata.db.mvcc.enabled", "true")
                                    .put("metadata.db.filename", metadataDatabase).build();

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


                    kinesisPort = startKinesis();
                    int dynamodbPort = createDynamodb();

                    StreamPlugin streamPlugin = new StreamPlugin("streaming", metastoreModule) {
                        @Override
                        public void setOptionalConfig(Map<String, String> optionalConfig) {
                            ImmutableMap.Builder<String, String> build = ImmutableMap.<String, String>builder()
                                    .put("target.connector_id", "rakam_raptor")
                                    .put("backup.provider", "file")
                                    .put("http-server.http.port", Integer.toString(ThreadLocalRandom.current().nextInt(1000, 10000)))
                                    .put("backup.directory", Files.createTempDir().getAbsolutePath())
                                    .put("storage.directory", Files.createTempDir().getAbsolutePath())
                                    .put("backup.timeout", "1m")
                                    .put("stream.source", "kinesis")
                                    .put("kinesis.stream", "rakam-events")
                                    .put("aws.kinesis-endpoint", "http://127.0.0.1:" + kinesisPort)
                                    .put("aws.dynamodb-endpoint", "http://127.0.0.1:" + dynamodbPort)
                                    .put("aws.secret-access-key", "AKIAIBZAIKH65T3ESNBQ")
                                    .put("aws.access-key", "JVKUio6AZTZ9oQgpbTlVeRcyhTo7zivi3oHa1IYg")
                                    .put("aws.region", "eu-central-1")
                                    .put("aws.enable-cloudwatch", "false")
                                    .put("kinesis.consumer-dynamodb-table", "rakamtest")
                                    .put("middleware.max-flush-records", "1")
                                    .put("stream.max-flush-records", "1");

                            ImmutableMap<String, String> b = build.build();
                            optionalConfig.entrySet().stream()
                                    .filter(f -> !b.containsKey(f.getKey()))
                                    .forEach(build::put);
                            super.setOptionalConfig(build.build());
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

                    System.out.println(prestoConfig.getAddress());
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
                                                if(kinesisProcess != null) {
                                                    kinesisProcess.destroy();
                                                }
                                                dynamodbServer.destroy();
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
                System.out.println("postgresql config: "+postgresqlConfig);
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
        if (kinesisProcess != null) {
            kinesisProcess.destroy();
        }
        if (dynamoDBServer != null) {
            dynamodbServer.destroy();
        }
    }

    private int startKinesis() throws Exception {
        Path mainDir = new File(getProperty("user.dir"), ".test/kinesalite").toPath();

        String nodePath = mainDir
                .resolve("node/node")
                .toFile().getAbsolutePath();

        String kinesalitePath = mainDir
                .resolve("node_modules/.bin/kinesalite")
                .toFile().getAbsolutePath();

        int kinesisPort = randomPort();
        kinesisProcess = new ProcessBuilder(of(nodePath, kinesalitePath, "--port", Integer.toString(kinesisPort)))
                .redirectErrorStream(true)
                .redirectOutput(INHERIT)
                .start();
        return kinesisPort;
    }

//    private int startKinesis() throws Exception {
//        return 4567;
//    }

    public int createDynamodb() throws Exception {
        int randomPort = randomPort();
        Path mainDir = new File(getProperty("user.dir"), ".test/dynamodb").toPath();

        dynamodbServer = new ProcessBuilder(of("java", format("-Djava.library.path=%s",
                        mainDir.resolve("DynamoDBLocal_lib").toFile().getAbsolutePath()),
                "-jar", mainDir.resolve("DynamoDBLocal.jar").toFile().getAbsolutePath(),
                "-inMemory", "--port", Integer.toString(randomPort)))
                .start();

        return randomPort;
    }

    private static int randomPort()
            throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    public int getKinesisPort() {
        return kinesisPort;
    }
}
