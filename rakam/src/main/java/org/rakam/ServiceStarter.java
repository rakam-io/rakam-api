package org.rakam;

import com.facebook.presto.jdbc.internal.guava.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.rakam.analysis.ContinuousQueryHttpService;
import org.rakam.analysis.stream.EventStreamHttpService;
import org.rakam.bootstrap.Bootstrap;
import org.rakam.collection.event.EventHttpService;
import org.rakam.config.ForHttpServer;
import org.rakam.config.HttpServerConfig;
import org.rakam.config.MetadataConfig;
import org.rakam.kume.Cluster;
import org.rakam.kume.ClusterBuilder;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.EventProcessor;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.user.UserHttpService;
import org.rakam.report.PrestoConfig;
import org.rakam.report.MaterializedViewHttpService;
import org.rakam.server.http.HttpServer;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.WebSocketService;
import org.rakam.util.HostAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        } else {
            System.setProperty("config", "config.properties");
        }

        ImmutableList.Builder<Module> builder = ImmutableList.builder();

//        for (File file : listFiles(new File("./"))) {
//            if (file.isDirectory()) {
//                loadPlugin(file.getAbsolutePath());
//            }
//        }

//        new ModuleInstaller() {
//            @Override
//            public void visit(RakamModule module) {
//                builder.add(module);
//            }
//        };

        ServiceLoader<RakamModule> modules = ServiceLoader.load(RakamModule.class);
        for (Module module : modules) {
            if (!(module instanceof RakamModule)) {
                throw new IllegalStateException(format("Modules must be subclasses of org.rakam.module.RakamModule: %s",
                        module.getClass().getName()));
            }
            RakamModule rakamModule = (RakamModule) module;
            builder.add(rakamModule);
        }

        builder.add(binder -> {
            binder.bind(Cluster.class).toProvider(() -> {
                return new ClusterBuilder().start();
            }).in(Scopes.SINGLETON);
        });

        builder.add(new ServiceRecipe());

        Bootstrap app = new Bootstrap(builder.build());
        app.requireExplicitBindings(false);

        Injector injector = app.strictConfig().initialize();

        HttpServerConfig httpConfig = injector.getInstance(HttpServerConfig.class);

        if(!httpConfig.getDisabled()) {
            HostAddress address = httpConfig.getAddress();

            NioEventLoopGroup eventExecutors = new NioEventLoopGroup();
            HttpServer httpServer = new HttpServer(
                    injector.getInstance(new Key<Set<HttpService>>() {}),
                    injector.getInstance(new Key<Set<WebSocketService>>() {}),
                    eventExecutors);

            httpServer.bind(address.getHostText(), address.getPort());
        }

        LOGGER.info("======== SERVER STARTED ========");
    }

    public static class ServiceRecipe extends AbstractConfigurationAwareModule {
        @Override
        protected void setup(Binder binder) {
            Multibinder.newSetBinder(binder, EventProcessor.class);
            Multibinder.newSetBinder(binder, EventMapper.class);

            Multibinder<HttpService> httpServices = Multibinder.newSetBinder(binder, HttpService.class);
            httpServices.addBinding().to(MaterializedViewHttpService.class);
            httpServices.addBinding().to(UserHttpService.class);
            httpServices.addBinding().to(EventHttpService.class);
            httpServices.addBinding().to(EventStreamHttpService.class);
            httpServices.addBinding().to(ContinuousQueryHttpService.class);

            Multibinder.newSetBinder(binder, WebSocketService.class);

            bindConfig(binder).to(HttpServerConfig.class);
            binder.bind(EventLoopGroup.class)
                    .annotatedWith(ForHttpServer.class)
                    .to(NioEventLoopGroup.class)
                    .in(Scopes.SINGLETON);

            bindConfig(binder).to(PrestoConfig.class);
            bindConfig(binder).to(MetadataConfig.class);
        }
    }
}