package org.rakam.event;

import com.facebook.presto.rakam.RakamRaptorPlugin;
import com.facebook.presto.rakam.stream.StreamPlugin;
import com.facebook.presto.rakam.stream.metadata.ForMetadata;
import com.facebook.presto.raptor.RaptorPlugin;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.testing.postgresql.TestingPostgreSqlServer;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.gaul.s3proxy.AuthenticationType;
import org.gaul.s3proxy.S3Proxy;
import org.gaul.s3proxy.S3ProxyConstants;
import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.aws.AWSConfig;
import org.rakam.config.JDBCConfig;
import org.rakam.presto.analysis.PrestoConfig;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Throwables.propagate;
import static com.google.common.collect.ImmutableList.of;
import static java.lang.ProcessBuilder.Redirect.INHERIT;
import static java.lang.String.format;
import static java.lang.System.getProperty;
import static org.gaul.s3proxy.AuthenticationType.AWS_V2_OR_V4;

public class TestingEnvironment
{

    private static PrestoConfig prestoConfig;
    private static TestingPrestoServer testingPrestoServer;
    private static TestingPostgreSqlServer testingPostgresqlServer;
    private static JDBCConfig postgresqlConfig;
    private static int kinesisPort;
    private static JDBCPoolDataSource metastore;
    private static Process kinesisProcess;
    private final S3ProxyLaunchInfo s3ProxyLaunchInfo;
    private Process dynamodbServer;

    public TestingEnvironment()
    {
        this(true);
    }

    public TestingEnvironment(boolean installMetadata)
    {
        try {
            s3ProxyLaunchInfo = startS3Proxy();
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }

//        AmazonS3Client amazonS3Client = new AmazonS3Client(getAWSConfig().getCredentials());
//        amazonS3Client.setEndpoint(getAWSConfig().getS3Endpoint());
//        amazonS3Client.createBucket("testing");

        try {
            if (testingPrestoServer == null) {
                synchronized (TestingEnvironment.class) {
                    testingPrestoServer = new TestingPrestoServer();

                    String metadataDatabase = Files.createTempDir().getAbsolutePath();
                    RaptorPlugin plugin = new RakamRaptorPlugin();

                    Module metastoreModule = new Module()
                    {
                        @Override
                        public void configure(Binder binder)
                        {
                        }

                        @Provides
                        @Singleton
                        @ForMetadata
                        public IDBI getDataSource()
                        {
                            return new DBI(format("jdbc:h2:mem:test%s;DB_CLOSE_DELAY=-1;mode=MySQL", System.nanoTime()));
                        }
                    };

                    kinesisPort = startKinesis();
                    int dynamodbPort = createDynamodbProcess();

                    StreamPlugin streamPlugin = new StreamPlugin("streaming", metastoreModule);

                    testingPrestoServer.installPlugin(plugin);
                    testingPrestoServer.installPlugin(streamPlugin);
                    testingPrestoServer.createCatalog("streaming", "streaming", ImmutableMap.<String, String>builder()
                            .put("target.connector_id", "rakam_raptor")
                            .put("backup.provider", "s3")
//                            .put("backup.s3.bucket", "testing")
                            .put("aws.s3-endpoint", s3ProxyLaunchInfo.getEndpoint().toString())
                            .put("stream.max-flush-duration", "0ms")
                            .put("http-server.http.port", Integer.toString(ThreadLocalRandom.current().nextInt(1000, 10000)))
                            .put("storage.directory", Files.createTempDir().getAbsolutePath())
                            .put("backup.timeout", "1m")
                            .put("stream.source", "kinesis")
                            .put("kinesis.stream", "rakam-events")
                            .put("aws.kinesis-endpoint", "http://127.0.0.1:" + kinesisPort)
                            .put("aws.dynamodb-endpoint", "http://127.0.0.1:" + dynamodbPort)
                            .put("aws.secret-access-key", s3ProxyLaunchInfo.getS3Credential())
                            .put("aws.access-key", s3ProxyLaunchInfo.getS3Identity())
                            .put("aws.region", "us-east-1")
                            .put("aws.enable-cloudwatch", "false")
                            .put("kinesis.consumer-dynamodb-table", "rakamtest")
                            .put("middleware.max-flush-records", "1")
                            .put("stream.max-flush-records", "1").build());

                    testingPrestoServer.createCatalog("rakam_raptor", "rakam_raptor", ImmutableMap.<String, String>builder()
                            .put("storage.data-directory", Files.createTempDir().getAbsolutePath())
                            .put("metadata.db.type", "h2")
                            .put("metadata.db.connections.wait", "10s")
                            .put("metadata.db.connections.max", "500")
                            .put("metadata.db.mvcc.enabled", "true")
                            .put("metadata.db.filename", metadataDatabase).build());

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
                                                if (kinesisProcess != null) {
                                                    kinesisProcess.destroy();
                                                }
                                                dynamodbServer.destroy();
                                            }
                                            catch (IOException e) {
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
                System.out.println("postgresql config: " + postgresqlConfig);
            }
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    public AWSConfig getAWSConfig()
    {
        return new AWSConfig()
                .setAccessKey(s3ProxyLaunchInfo.getS3Identity())
                .setSecretAccessKey(s3ProxyLaunchInfo.getS3Credential())
                .setRegion("us-east-1")
                .setS3Endpoint(s3ProxyLaunchInfo.getEndpoint().toString());
    }

    public JDBCPoolDataSource getPrestoMetastore()
    {
        return metastore;
    }

    public JDBCConfig getPostgresqlConfig()
    {
        if (postgresqlConfig == null) {
            throw new UnsupportedOperationException();
        }
        return postgresqlConfig;
    }

    public PrestoConfig getPrestoConfig()
    {
        return prestoConfig;
    }

    public void close()
            throws Exception
    {
        testingPrestoServer.close();
        if (testingPostgresqlServer != null) {
            testingPostgresqlServer.close();
        }
        if (kinesisProcess != null) {
            kinesisProcess.destroy();
        }
    }

    private int startKinesis()
            throws Exception
    {
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

    public static int randomPort()
            throws IOException
    {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    public int createDynamodbProcess()
            throws Exception
    {
        int randomPort = randomPort();
        Path mainDir = new File(getProperty("user.dir"), ".test/dynamodb").toPath();

        dynamodbServer = new ProcessBuilder(of("java", format("-Djava.library.path=%s",
                mainDir.resolve("DynamoDBLocal_lib").toFile().getAbsolutePath()),
                "-jar", mainDir.resolve("DynamoDBLocal.jar").toFile().getAbsolutePath(),
                "-inMemory", "--port", Integer.toString(randomPort)))
                .start();

        return randomPort;
    }

    public int getKinesisPort()
    {
        return kinesisPort;
    }

    static S3ProxyLaunchInfo startS3Proxy()
            throws Exception
    {
        S3ProxyLaunchInfo info = new S3ProxyLaunchInfo();

        try (InputStream is = Resources.asByteSource(Resources.getResource(
                "s3proxy.conf")).openStream()) {
            info.getProperties().load(is);
        }

        String provider = info.getProperties().getProperty(
                Constants.PROPERTY_PROVIDER);
        String identity = info.getProperties().getProperty(
                Constants.PROPERTY_IDENTITY);
        String credential = info.getProperties().getProperty(
                Constants.PROPERTY_CREDENTIAL);
        String endpoint = info.getProperties().getProperty(
                Constants.PROPERTY_ENDPOINT);
        AuthenticationType s3ProxyAuthorization = AWS_V2_OR_V4;
        info.s3Identity = info.getProperties().getProperty(
                S3ProxyConstants.PROPERTY_IDENTITY);
        info.s3Credential = info.getProperties().getProperty(
                S3ProxyConstants.PROPERTY_CREDENTIAL);
        info.endpoint = new URI(info.getProperties().getProperty(
                S3ProxyConstants.PROPERTY_ENDPOINT));
        String secureEndpoint = info.getProperties().getProperty(
                S3ProxyConstants.PROPERTY_SECURE_ENDPOINT);
        String keyStorePath = info.getProperties().getProperty(
                S3ProxyConstants.PROPERTY_KEYSTORE_PATH);
        String keyStorePassword = info.getProperties().getProperty(
                S3ProxyConstants.PROPERTY_KEYSTORE_PASSWORD);
        String virtualHost = info.getProperties().getProperty(
                S3ProxyConstants.PROPERTY_VIRTUAL_HOST);

        ContextBuilder builder = ContextBuilder
                .newBuilder(provider)
                .credentials(identity, credential)
                .modules(ImmutableList.<Module>of(new SLF4JLoggingModule()))
                .overrides(info.getProperties());
        if (!Strings.isNullOrEmpty(endpoint)) {
            builder.endpoint(endpoint);
        }
        BlobStoreContext context = builder.build(BlobStoreContext.class);
        info.blobStore = context.getBlobStore();

        S3Proxy.Builder s3ProxyBuilder = S3Proxy.builder()
                .blobStore(info.getBlobStore())
                .endpoint(info.getEndpoint());
        if (secureEndpoint != null) {
            s3ProxyBuilder.secureEndpoint(new URI(secureEndpoint));
        }
        if (info.getS3Identity() != null || info.getS3Credential() != null) {
            s3ProxyBuilder.awsAuthentication(s3ProxyAuthorization,
                    info.getS3Identity(), info.getS3Credential());
        }
        if (keyStorePath != null || keyStorePassword != null) {
            s3ProxyBuilder.keyStore(
                    Resources.getResource(keyStorePath).toString(),
                    keyStorePassword);
        }
        if (virtualHost != null) {
            s3ProxyBuilder.virtualHost(virtualHost);
        }
        info.s3Proxy = s3ProxyBuilder.build();
        info.s3Proxy.start();
        while (!info.s3Proxy.getState().equals(AbstractLifeCycle.STARTED)) {
            Thread.sleep(1);
        }

        // reset endpoint to handle zero port
        info.endpoint = new URI(info.endpoint.getScheme(),
                info.endpoint.getUserInfo(), info.endpoint.getHost(),
                info.s3Proxy.getPort(), info.endpoint.getPath(),
                info.endpoint.getQuery(), info.endpoint.getFragment());

        return info;
    }

    static final class S3ProxyLaunchInfo
    {
        private S3Proxy s3Proxy;
        private Properties properties = new Properties();
        private String s3Identity;
        private String s3Credential;
        private BlobStore blobStore;
        private URI endpoint;

        S3Proxy getS3Proxy()
        {
            return s3Proxy;
        }

        Properties getProperties()
        {
            return properties;
        }

        String getS3Identity()
        {
            return s3Identity;
        }

        String getS3Credential()
        {
            return s3Credential;
        }

        BlobStore getBlobStore()
        {
            return blobStore;
        }

        URI getEndpoint()
        {
            return endpoint;
        }
    }
}
