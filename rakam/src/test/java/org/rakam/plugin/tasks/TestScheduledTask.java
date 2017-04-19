package org.rakam.plugin.tasks;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.rakam.TestingConfigManager;
import org.rakam.analysis.InMemoryApiKeyService;
import org.rakam.analysis.InMemoryEventStore;
import org.rakam.analysis.InMemoryMetastore;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.analysis.metadata.SchemaChecker;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.config.ProjectConfig;
import org.rakam.util.javascript.JSCodeLoggerService;
import org.rakam.collection.JsonEventDeserializer;
import org.rakam.util.javascript.JSCodeCompiler;
import org.rakam.config.JDBCConfig;
import org.rakam.plugin.RAsyncHttpClient;
import org.rakam.plugin.tasks.ScheduledTaskHttpService.Environment;
import org.rakam.ui.ScheduledTaskUIHttpService;
import org.rakam.ui.ScheduledTaskUIHttpService.Parameter;
import org.rakam.util.JsonHelper;
import org.rakam.util.javascript.JSConfigManager;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;

import static com.google.common.io.ByteStreams.toByteArray;
import static java.lang.String.format;
import static org.rakam.collection.FieldType.STRING;
import static org.rakam.plugin.tasks.ScheduledTaskHttpService.run;

public class TestScheduledTask
{

    @Test(enabled = false)
//    @Test()
    public void testName()
            throws Exception
    {
        FieldDependencyBuilder.FieldDependency fieldDependency = new FieldDependencyBuilder().build();
        JSCodeCompiler.TestLogger logger = new JSCodeCompiler.TestLogger();
        TestingConfigManager testingConfigManager = new TestingConfigManager();
        JSCodeCompiler.IJSConfigManager ijsConfigManager = new JSConfigManager(testingConfigManager, "test", null);

        InMemoryApiKeyService apiKeyService = new InMemoryApiKeyService();
        InMemoryMetastore metastore = new InMemoryMetastore(apiKeyService);
        SchemaChecker schemaChecker = new SchemaChecker(metastore, new FieldDependencyBuilder().build());
        JsonEventDeserializer testingEventDeserializer = new JsonEventDeserializer(
                metastore,
                apiKeyService,
                testingConfigManager,
                schemaChecker,
                new ProjectConfig(),
                fieldDependency);
        metastore.createProject("test");
        String metadataDatabase = Files.createTempDir().getAbsolutePath();

        JDBCPoolDataSource sa = JDBCPoolDataSource.getOrCreateDataSource(new JDBCConfig().setUrl("jdbc:h2:" + metadataDatabase)
                .setUsername("sa").setPassword(""));

        ScheduledTaskUIHttpService.ScheduledTask task = JsonHelper.read(toByteArray(this.getClass().getResource("/scheduled-task/facebook-ads/config.json").openStream()), ScheduledTaskUIHttpService.ScheduledTask.class);

        task.parameters.get("collection").value = "test";
//        task.parameters.computeIfAbsent("developer_token", (k) -> new Parameter(STRING, null, null, null, null)).value = "";
//        task.parameters.computeIfAbsent("customer_id", (k) -> new Parameter(STRING, null, null, null, null)).value = "";
//        task.parameters.computeIfAbsent("refresh_token", (k) -> new Parameter(STRING, null, null, null, null)).value = "";

        task.parameters.computeIfAbsent("account_id", (k) -> new Parameter(STRING, null, false, null, null, null, false)).value = "";
        task.parameters.computeIfAbsent("access_token", (k) -> new Parameter(STRING, null, false, null, null, null, false)).value = "";

        InMemoryEventStore eventStore = new InMemoryEventStore();

        JSCodeCompiler jsCodeCompiler = new JSCodeCompiler(testingConfigManager,
                RAsyncHttpClient.create(1000, ""),
                (project, prefix) -> new JSCodeLoggerService(sa).createLogger(project, prefix),
                true, true);

        ListenableFuture<Void> test = run(jsCodeCompiler, MoreExecutors.listeningDecorator(MoreExecutors.newDirectExecutorService()), "test", "load('../rakam-ui/src/main/resources/scheduled-task/facebook-ads/script.js')",
                task.parameters, logger, ijsConfigManager, testingEventDeserializer, eventStore, ImmutableList.of());

        CompletableFuture<Environment> future = new CompletableFuture<>();
        Futures.addCallback(test, new FutureCallback<Void>() {
            @Override
            public void onSuccess(Void v)
            {
                if (eventStore.getEvents().isEmpty()) {
                    logger.info("No event is returned");
                }
                else {
                    logger.info(format("Successfully got %d events: %s: %s", eventStore.getEvents().size(),
                            eventStore.getEvents().get(0).collection(),
                            eventStore.getEvents().get(0).properties()));
                }

                future.complete(new Environment(logger.getEntries(), testingConfigManager.getTable().row("test")));
            }

            @Override
            public void onFailure(Throwable throwable)
            {
                future.completeExceptionally(throwable);
            }
        });

        System.out.println(future.join());
    }
}
