package org.rakam;

import com.google.inject.Injector;
import com.google.inject.Scopes;
import org.rakam.collection.CollectionModule;
import org.rakam.kume.Cluster;
import org.rakam.kume.ClusterBuilder;
import org.rakam.server.http.HttpServer;
import org.rakam.util.bootstrap.Bootstrap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by buremba on 21/12/13.
 */

public class ServiceStarter {
    final static Logger LOGGER = LoggerFactory.getLogger(Cluster.class);

    public static void main(String[] args) throws Throwable {

//        Runtime.getRuntime().addShutdownHook(new Thread() {
//            @Override
//            public void bind() {
//                System.out.println("exiting..");
//                TODO: gracefully exit.
//                System.exit(0);
//            }
//        });

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

}