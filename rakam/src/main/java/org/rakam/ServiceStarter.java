package org.rakam;

import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.SubscriberExceptionContext;
import com.google.common.eventbus.SubscriberExceptionHandler;
import com.google.inject.*;
import com.google.inject.matcher.Matchers;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.multibindings.OptionalBinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.google.inject.spi.InjectionListener;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.log.Logger;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.swagger.models.Tag;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.FlywayException;
import org.rakam.analysis.*;
import org.rakam.analysis.datasource.CustomDataSourceConfig;
import org.rakam.analysis.metadata.SchemaChecker;
import org.rakam.bootstrap.ProxyBootstrap;
import org.rakam.collection.*;
import org.rakam.collection.FieldDependencyBuilder.FieldDependency;
import org.rakam.config.EncryptionConfig;
import org.rakam.config.MetadataConfig;
import org.rakam.config.ProjectConfig;
import org.rakam.config.TaskConfig;
import org.rakam.http.ForHttpServer;
import org.rakam.http.HttpServerConfig;
import org.rakam.http.OptionMethodHttpService;
import org.rakam.http.WebServiceModule;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.InjectionHook;
import org.rakam.plugin.LockServiceProvider;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.stream.EventStreamConfig;
import org.rakam.plugin.user.AbstractUserService;
import org.rakam.plugin.user.UserStorage;
import org.rakam.plugin.user.mailbox.UserMailboxStorage;
import org.rakam.postgresql.analysis.FastGenericFunnelQueryExecutor;
import org.rakam.report.QueryExecutorService;
import org.rakam.report.realtime.RealTimeConfig;
import org.rakam.server.http.HttpRequestHandler;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.WebSocketService;
import org.rakam.ui.ActiveModuleListBuilder;
import org.rakam.util.NotFoundHandler;
import org.rakam.util.RAsyncHttpClient;
import org.rakam.util.javascript.JSCodeJDBCLoggerService;
import org.rakam.util.javascript.JSLoggerService;
import org.rakam.util.lock.LockService;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.Clock;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.lang.String.format;

public final class ServiceStarter {
    private final static Logger LOGGER = Logger.get(ServiceStarter.class);
    public static String RAKAM_VERSION;

    static {
        Properties properties = new Properties();
        InputStream inputStream;
        try {
            URL resource = ServiceStarter.class.getResource("/git.properties");
            if (resource == null) {
                LOGGER.warn("git.properties doesn't exist.");
            } else {
                inputStream = resource.openStream();
                properties.load(inputStream);

                try {
                    RAKAM_VERSION = properties.get("git.commit.id.describe-short").toString().split("-", 2)[0];
                } catch (Exception e) {
                    LOGGER.warn(e, "Error while parsing git.properties");
                }
            }
        } catch (IOException e) {
            LOGGER.warn(e, "Error while reading git.properties");
        }
    }

    private ServiceStarter()
            throws InstantiationException {
        throw new InstantiationException("The class is not created for instantiation");
    }

    public static void main(String[] args)
            throws Throwable {
        if (args.length > 0) {
            System.setProperty("config", args[0]);
        }

        ProxyBootstrap app = new ProxyBootstrap(getModules());
        app.requireExplicitBindings(true);
        Injector injector = app.strictConfig().initialize();

        Set<InjectionHook> hooks = injector.getInstance(
                Key.get(new TypeLiteral<Set<InjectionHook>>() {
                }));
        hooks.forEach(InjectionHook::call);

        HttpServerConfig httpConfig = injector.getInstance(HttpServerConfig.class);

        if (!httpConfig.getDisabled()) {
            WebServiceModule webServiceModule = injector.getInstance(WebServiceModule.class);
            injector.createChildInjector(webServiceModule);
        }

        LOGGER.info("======== SERVER STARTED ========");
    }

    public static Set<Module> getModules() {
        ImmutableSet.Builder<Module> builder = ImmutableSet.builder();

        ServiceLoader<RakamModule> modules = ServiceLoader.load(RakamModule.class);
        for (Module module : modules) {
            if (!(module instanceof RakamModule)) {
                throw new IllegalStateException(format("Modules must be subclasses of org.rakam.module.RakamModule: %s",
                        module.getClass().getName()));
            }
            RakamModule rakamModule = (RakamModule) module;
            builder.add(rakamModule);
        }

        builder.add(new ServiceRecipe());
        return builder.build();
    }

    public static class FieldDependencyProvider
            implements Provider<FieldDependency> {

        private final Set<EventMapper> eventMappers;

        @Inject
        public FieldDependencyProvider(Set<EventMapper> eventMappers) {
            this.eventMappers = eventMappers;
        }

        @Override
        public FieldDependency get() {
            FieldDependencyBuilder builder = new FieldDependencyBuilder();
            eventMappers.stream().forEach(mapper -> mapper.addFieldDependency(builder));
            return builder.build();
        }
    }

    public static class ServiceRecipe
            extends AbstractConfigurationAwareModule {
        @Override
        protected void setup(Binder binder) {
            binder.bind(Clock.class).toInstance(Clock.systemUTC());
//            binder.bind(FlywayExecutor.class).asEagerSingleton();
            binder.bind(LockService.class).toProvider(LockServiceProvider.class);
            binder.bind(FieldDependency.class).toProvider(FieldDependencyProvider.class).in(Scopes.SINGLETON);

            Multibinder.newSetBinder(binder, EventMapper.class);
            Multibinder.newSetBinder(binder, InjectionHook.class);
            OptionalBinder.newOptionalBinder(binder, AbstractUserService.class);
            OptionalBinder.newOptionalBinder(binder, UserStorage.class);
            OptionalBinder.newOptionalBinder(binder, UserMailboxStorage.class);

            EventBus eventBus = new EventBus(new SubscriberExceptionHandler() {
                Logger logger = Logger.get("System Event Listener");

                @Override
                public void handleException(Throwable exception, SubscriberExceptionContext context) {
                    logger.error(exception, "Could not dispatch event: " +
                            context.getSubscriber() + " to " + context.getSubscriberMethod(), exception.getCause());
                }
            });
            binder.bind(EventBus.class).toInstance(eventBus);

            binder.bindListener(Matchers.any(), new TypeListener() {
                public void hear(TypeLiteral typeLiteral, TypeEncounter typeEncounter) {
                    typeEncounter.register((InjectionListener) i -> eventBus.register(i));
                }
            });

            Multibinder<Tag> tags = Multibinder.newSetBinder(binder, Tag.class);
            tags.addBinding().toInstance(new Tag().name("admin").description("System related actions").externalDocs(MetadataConfig.centralDocs));
            tags.addBinding().toInstance(new Tag().name("collect").description("Collect data").externalDocs(MetadataConfig.centralDocs));
            tags.addBinding().toInstance(new Tag().name("query").description("Analyze data").externalDocs(MetadataConfig.centralDocs));
            tags.addBinding().toInstance(new Tag().name("materialized-view").description("Materialized view").externalDocs(MetadataConfig.centralDocs));

            // Register these interfaces to MultiBinder
            Multibinder.newSetBinder(binder, EventMapper.class);

            Multibinder.newSetBinder(binder, RequestPreProcessorItem.class);

            Multibinder<CustomParameter> customParameters = Multibinder.newSetBinder(binder, CustomParameter.class);
            customParameters.addBinding().toProvider(ProjectPermissionParameterProvider.class);

            binder.bind(QueryHttpService.class).asEagerSingleton();
            binder.bind(QueryExecutorService.class).asEagerSingleton();
            configBinder(binder).bindConfig(TaskConfig.class);
            configBinder(binder).bindConfig(EventStreamConfig.class);
            configBinder(binder).bindConfig(RealTimeConfig.class);
            binder.bind(ActiveModuleListBuilder.class).asEagerSingleton();

            Multibinder<HttpService> httpServices = Multibinder.newSetBinder(binder, HttpService.class);
            httpServices.addBinding().to(AdminHttpService.class);
            httpServices.addBinding().to(ProjectHttpService.class);
            httpServices.addBinding().to(MaterializedViewHttpService.class);
            httpServices.addBinding().to(EventCollectionHttpService.class);
            httpServices.addBinding().to(QueryHttpService.class);
            httpServices.addBinding().to(OptionMethodHttpService.class);

            Multibinder.newSetBinder(binder, WebSocketService.class);

            binder.bind(AvroEventDeserializer.class);
            binder.bind(CsvEventDeserializer.class);
            binder.bind(EventListDeserializer.class);
            binder.bind(JsonEventDeserializer.class);

            binder.bind(FastGenericFunnelQueryExecutor.class);

            configBinder(binder).bindConfig(HttpServerConfig.class);
            configBinder(binder).bindConfig(ProjectConfig.class);
            configBinder(binder).bindConfig(EncryptionConfig.class);
            configBinder(binder).bindConfig(CustomDataSourceConfig.class);

            binder.bind(SchemaChecker.class).asEagerSingleton();

            binder.bind(JSLoggerService.class).to(JSCodeJDBCLoggerService.class);
            binder.bind(JSCodeJDBCLoggerService.class);

            binder.bind(RAsyncHttpClient.class)
                    .annotatedWith(Names.named("rakam-client"))
                    .toProvider(() -> RAsyncHttpClient.create(1000 * 60 * 10, "rakam-custom-script"))
                    .in(Scopes.SINGLETON);

            OptionalBinder.newOptionalBinder(binder,
                    Key.get(HttpRequestHandler.class, NotFoundHandler.class));

            binder.bind(EventLoopGroup.class)
                    .annotatedWith(ForHttpServer.class)
                    .to(NioEventLoopGroup.class)
                    .in(Scopes.SINGLETON);

            binder.bind(WebServiceModule.class);
        }
    }

    public static class ProjectPermissionParameterProvider
            implements Provider<CustomParameter> {

        private final ApiKeyService apiKeyService;

        @Inject
        public ProjectPermissionParameterProvider(ApiKeyService apiKeyService) {
            this.apiKeyService = apiKeyService;
        }

        @Override
        public CustomParameter get() {
            return new CustomParameter("project",
                    method -> new WebServiceModule.ProjectPermissionIRequestParameter(apiKeyService, method));
        }
    }

    public static class FlywayExecutor {
        @Inject
        public FlywayExecutor(@Named("report.metadata.store.jdbc") JDBCPoolDataSource config) {
            Flyway flyway = new Flyway();
            flyway.setBaselineOnMigrate(true);
            flyway.setDataSource(config);
            flyway.setLocations("db/migration/report");
            flyway.setTable("schema_version_report");
            try {
                flyway.migrate();
            } catch (FlywayException e) {
                flyway.repair();
            }
        }
    }
}