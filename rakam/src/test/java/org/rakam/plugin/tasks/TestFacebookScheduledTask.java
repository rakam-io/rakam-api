package org.rakam.plugin.tasks;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.io.Files;
import io.netty.channel.epoll.Epoll;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.rakam.TestingConfigManager;
import org.rakam.analysis.InMemoryApiKeyService;
import org.rakam.analysis.InMemoryMetastore;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.metadata.SchemaChecker;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.collection.FieldType;
import org.rakam.collection.JsonEventDeserializer;
import org.rakam.collection.util.JSCodeCompiler;
import org.rakam.config.JDBCConfig;
import org.rakam.plugin.RAsyncHttpClient;
import org.rakam.plugin.tasks.ScheduledTaskHttpService;
import org.rakam.ui.ScheduledTaskUIHttpService;
import org.rakam.util.JsonHelper;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.io.ByteStreams.toByteArray;
import static java.lang.String.format;
import static org.rakam.plugin.tasks.ScheduledTaskHttpService.run;

public class TestFacebookScheduledTask
{

    @Test
    public void testName()
            throws Exception
    {
        FieldDependencyBuilder.FieldDependency fieldDependency = new FieldDependencyBuilder().build();
        JSCodeCompiler.TestLogger logger = new JSCodeCompiler.TestLogger();
        TestingConfigManager testingConfigManager = new TestingConfigManager();
        JSCodeCompiler.IJSConfigManager ijsConfigManager = new JSCodeCompiler.JSConfigManager(testingConfigManager, "test", null);

        InMemoryApiKeyService apiKeyService = new InMemoryApiKeyService();
        InMemoryMetastore metastore = new InMemoryMetastore(apiKeyService);
        SchemaChecker schemaChecker = new SchemaChecker(metastore, new FieldDependencyBuilder().build());
        JsonEventDeserializer testingEventDeserializer = new JsonEventDeserializer(metastore,
                apiKeyService,
                testingConfigManager,
                schemaChecker,
                fieldDependency);
        metastore.createProject("test");

        AsyncHttpClientConfig cf = new DefaultAsyncHttpClientConfig.Builder()
                .setRequestTimeout(100000)
                .setUserAgent("test")
                .setUseNativeTransport(Epoll.isAvailable())
                .build();

        String metadataDatabase = Files.createTempDir().getAbsolutePath();

        JDBCPoolDataSource sa = JDBCPoolDataSource.getOrCreateDataSource(new JDBCConfig().setUrl("jdbc:h2:"+metadataDatabase)
                .setUsername("sa").setPassword(""));

        Map<String, ScheduledTaskUIHttpService.Parameter> config = JsonHelper.read(toByteArray(this.getClass().getResource("/scheduled-task/facebook-ads/config.json").openStream()),
                new TypeReference<Map<String, String>>() {}).entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> new ScheduledTaskUIHttpService.Parameter(FieldType.STRING,  e.getValue(), null, null)));

        JSCodeCompiler jsCodeCompiler = new JSCodeCompiler(testingConfigManager, sa, new RAsyncHttpClient(new DefaultAsyncHttpClient(cf)), true);
        CompletableFuture<ScheduledTaskHttpService.Environment> future =
                run(jsCodeCompiler, Runnable::run, "test", "load('src/test/resources/scheduled-task/facebook-ads/script.js')", config, logger, ijsConfigManager, testingEventDeserializer).thenApply(eventList -> {
            if (eventList == null || eventList.events.isEmpty()) {
                logger.info("No event is returned");
            }
            else {
                logger.info(format("Successfully got %d events: %s: %s", eventList.events.size(), eventList.events.get(0).collection(), eventList.events.get(0).properties()));
            }

            return new ScheduledTaskHttpService.Environment(logger.getEntries(), testingConfigManager.getTable().row("test"));
        });

        System.out.println(future.join());
    }
}
