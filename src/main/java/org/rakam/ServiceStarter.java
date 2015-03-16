package org.rakam;

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.rakam.collection.CollectionModule;
import org.rakam.collection.event.EventCollectorHttpService;
import org.rakam.kume.Cluster;
import org.rakam.kume.ClusterBuilder;
import org.rakam.plugin.EventMapper;
import org.rakam.plugin.EventProcessor;
import org.rakam.plugin.RakamModule;
import org.rakam.plugin.user.ActorCollectorService;
import org.rakam.report.ReportHttpService;
import org.rakam.server.http.ForHttpServer;
import org.rakam.server.http.HttpServer;
import org.rakam.server.http.HttpServerConfig;
import org.rakam.server.http.HttpService;
import org.rakam.util.bootstrap.Bootstrap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ServiceLoader;

import static io.airlift.configuration.ConfigurationModule.bindConfig;

/**
 * Created by buremba on 21/12/13.
 */

public class ServiceStarter {
    final static Logger LOGGER = LoggerFactory.getLogger(Cluster.class);

    public static void main(String[] args) throws Throwable {
        if (args.length > 0) {
            System.setProperty("config", args[0]);
        } else {
            System.setProperty("config", "config.properties");
        }

        Bootstrap app = new Bootstrap(
                new CollectionModule(),
                binder -> {
                    binder.bind(Cluster.class).toProvider(() -> {
                        return new ClusterBuilder().start();
                    }).in(Scopes.SINGLETON);
                },
                new ServiceRecipe());

        app.requireExplicitBindings(false);

        Injector injector = app.strictConfig().initialize();

        HttpServer httpServer = injector.getInstance(HttpServer.class);
        if(!httpServer.isDisabled()) {
            httpServer.bind();
        }

        LOGGER.info("======== SERVER STARTED ========");
    }

    public static class ServiceRecipe extends AbstractConfigurationAwareModule {
        @Override
        protected void setup(Binder binder) {
            Multibinder.newSetBinder(binder, EventProcessor.class);
            Multibinder.newSetBinder(binder, EventMapper.class);

            Multibinder<HttpService> httpServices = Multibinder.newSetBinder(binder, HttpService.class);
            httpServices.addBinding().to(ReportHttpService.class);
            httpServices.addBinding().to(ActorCollectorService.class);
            httpServices.addBinding().to(EventCollectorHttpService.class);

            ServiceLoader<RakamModule> modules = ServiceLoader.load(RakamModule.class);

            Multibinder<RakamModule> rakamModuleBinder = Multibinder.newSetBinder(binder, RakamModule.class);
            for (Module module : modules) {
                if (!(module instanceof RakamModule)) {
                    binder.addError("Modules must be subclasses of org.rakam.module.RakamModule: %s", module.getClass().getName());
                    continue;
                }
                RakamModule rakamModule = (RakamModule) module;
                super.install(rakamModule);
                rakamModuleBinder.addBinding().toInstance(rakamModule);
            }

            bindConfig(binder).to(HttpServerConfig.class);
            binder.bind(HttpServer.class).asEagerSingleton();
            binder.bind(EventLoopGroup.class)
                    .annotatedWith(ForHttpServer.class)
                    .to(NioEventLoopGroup.class)
                    .in(Scopes.SINGLETON);
        }
    }
}