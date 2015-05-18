package org.rakam;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
import com.wordnik.swagger.models.Contact;
import com.wordnik.swagger.models.Info;
import com.wordnik.swagger.models.License;
import com.wordnik.swagger.models.Swagger;
import com.wordnik.swagger.models.auth.ApiKeyAuthDefinition;
import com.wordnik.swagger.models.auth.In;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.rakam.analysis.ContinuousQueryHttpService;
import org.rakam.analysis.stream.EventStreamHttpService;
import org.rakam.bootstrap.Bootstrap;
import org.rakam.collection.event.EventHttpService;
import org.rakam.config.ForHttpServer;
import org.rakam.config.HttpServerConfig;
import org.rakam.config.PluginConfig;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.EventProcessor;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.user.UserHttpService;
import org.rakam.report.MaterializedViewHttpService;
import org.rakam.report.PrestoConfig;
import org.rakam.report.QueryHttpService;
import org.rakam.server.http.HttpServer;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.WebSocketService;
import org.rakam.util.HostAddress;
import org.rakam.util.JsonHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Arrays;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;

import static io.airlift.configuration.ConfigurationModule.bindConfig;
import static java.lang.String.format;

/**
 * Created by buremba on 21/12/13.
 */
public class ServiceStarter {
    final static Logger LOGGER = LoggerFactory.getLogger(ServiceStarter.class);

    public static void main(String[] args) throws Throwable {
        if (args.length > 0) {
            System.setProperty("config", args[0]);
        }

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

//        builder.add(binder -> {
//            binder.bind(Cluster.class).toProvider(() -> {
//                return new ClusterBuilder().start();
//            }).in(Scopes.SINGLETON);
//        });

        builder.add(new ServiceRecipe());

        Bootstrap app = new Bootstrap(builder.build());
        app.requireExplicitBindings(false);

        Injector injector = app.strictConfig().initialize();

        HttpServerConfig httpConfig = injector.getInstance(HttpServerConfig.class);

        if(!httpConfig.getDisabled()) {
            WebServiceRecipe webServiceRecipe = injector.getInstance(WebServiceRecipe.class);
            injector.createChildInjector(webServiceRecipe);
        }

        LOGGER.info("======== SERVER STARTED ========");
    }

    @Singleton
    public static class WebServiceRecipe extends AbstractModule {

        private final Set<WebSocketService> webSocketServices;
        private final Set<HttpService> httpServices;
        private final HttpServerConfig config;

        @Inject
        public WebServiceRecipe(Set<HttpService> httpServices, Set<WebSocketService> webSocketServices, HttpServerConfig config) {
            this.httpServices = httpServices;
            this.webSocketServices = webSocketServices;
            this.config = config;
        }

        @Override
        protected void configure() {

            Info info = new Info()
                    .title("Rakam API Documentation")
                    .version("1.0")
                    .description("An analytics platform API that lets you create your own analytics services.")
                    .contact(new Contact().email("contact@getrakam.com"))
                    .license(new License()
                            .name("Apache License 2.0")
                            .url("http://www.apache.org/licenses/LICENSE-2.0.html"));

            Swagger swagger = new Swagger().info(info)
                    .securityDefinition("api_key", new ApiKeyAuthDefinition("api_key", In.HEADER));

            NioEventLoopGroup eventExecutors = new NioEventLoopGroup();

            HttpServer httpServer = new HttpServer(
                    httpServices,
                    webSocketServices, swagger,
                    eventExecutors, JsonHelper.getMapper());

            HostAddress address = config.getAddress();
            try {
                httpServer.bind(address.getHostText(), address.getPort());
            } catch (InterruptedException e) {
                addError(e);
                return;
            }

            binder().bind(HttpServer.class).toInstance(httpServer);
        }
    }
    public static class ServiceRecipe extends AbstractConfigurationAwareModule {
        @Override
        protected void setup(Binder binder) {
            binder.bind(Clock.class).toInstance(Clock.systemUTC());

            Multibinder.newSetBinder(binder, EventProcessor.class);
            Multibinder.newSetBinder(binder, EventMapper.class);

            Multibinder<HttpService> httpServices = Multibinder.newSetBinder(binder, HttpService.class);
            httpServices.addBinding().to(ProjectHttpService.class);
            httpServices.addBinding().to(MaterializedViewHttpService.class);
            httpServices.addBinding().to(UserHttpService.class);
            httpServices.addBinding().to(EventHttpService.class);
            httpServices.addBinding().to(EventStreamHttpService.class);
            httpServices.addBinding().to(ContinuousQueryHttpService.class);
            httpServices.addBinding().to(QueryHttpService.class);

            Multibinder.newSetBinder(binder, WebSocketService.class);

            bindConfig(binder).to(HttpServerConfig.class);
            PluginConfig pluginConfig = buildConfigObject(PluginConfig.class);

            try {
                new PluginInstaller(pluginConfig, this::install)
                        .loadPlugins();
            } catch (Exception e) {
                binder.addError(e);
            }

            binder.bind(EventLoopGroup.class)
                    .annotatedWith(ForHttpServer.class)
                    .to(NioEventLoopGroup.class)
                    .in(Scopes.SINGLETON);

            bindConfig(binder).to(PrestoConfig.class);
            bindConfig(binder).to(MetadataConfig.class);
            binder.bind(WebServiceRecipe.class);
        }
    }

    public static boolean isAccessibleDirectory(Path directory) {
        if (!Files.exists(directory)) {
            LOGGER.debug("[{}] directory does not exist.", directory.toAbsolutePath());
            return false;
        }
        if (!Files.isDirectory(directory)) {
            LOGGER.debug("[{}] should be a directory but is not.", directory.toAbsolutePath());
            return false;
        }
        if (!Files.isReadable(directory)) {
            LOGGER.debug("[{}] directory is not readable.", directory.toAbsolutePath());
            return false;
        }
        return true;
    }

    private void loadPluginsIntoClassLoader() throws IOException {
        File homeFile = new File(System.getProperty("user.dir"));

        Path pluginsDirectory = new File(homeFile, "plugins").toPath();
        if (!isAccessibleDirectory(pluginsDirectory)) {
            return;
        }

        ClassLoader classLoader = this.getClass().getClassLoader();
        Class classLoaderClass = classLoader.getClass();
        Method addURL = null;
        while (!classLoaderClass.equals(Object.class)) {
            try {
                addURL = classLoaderClass.getDeclaredMethod("addURL", URL.class);
                addURL.setAccessible(true);
                break;
            } catch (NoSuchMethodException e) {
                // no method, try the parent
                classLoaderClass = classLoaderClass.getSuperclass();
            }
        }
        if (addURL == null) {
            LOGGER.debug("failed to find addURL method on classLoader [" + classLoader + "] to add methods");
            return;
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(pluginsDirectory)) {

            for (Path plugin : stream) {
                // We check that subdirs are directories and readable
                if (!isAccessibleDirectory(plugin)) {
                    continue;
                }

                LOGGER.trace("--- adding plugin [{}]", plugin.toAbsolutePath());

                try {
                    // add the root
                    addURL.invoke(classLoader, plugin.toUri().toURL());
                    // gather files to add
                    List<Path> libFiles = Lists.newArrayList();
                    libFiles.addAll(Arrays.asList(files(plugin)));
                    Path libLocation = plugin.resolve("lib");
                    if (Files.isDirectory(libLocation)) {
                        libFiles.addAll(Arrays.asList(files(libLocation)));
                    }

                    // if there are jars in it, add it as well
                    for (Path libFile : libFiles) {
                        addURL.invoke(classLoader, libFile.toUri().toURL());
                    }
                } catch (Throwable e) {
                    LOGGER.warn("failed to add plugin [" + plugin + "]", e);
                }
            }
        }
    }

    private Path[] files(Path from) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(from)) {
            return Iterators.toArray(stream.iterator(), Path.class);
        }
    }
}