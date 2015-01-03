package org.rakam;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.rakam.kume.Cluster;
import org.rakam.kume.ClusterBuilder;
import org.rakam.server.WebServer;
import org.rakam.server.http.HTTPServer;

import java.io.FileNotFoundException;

/**
 * Created by buremba on 21/12/13.
 */

public class ServiceStarter {

    public static void main(String[] args) throws FileNotFoundException, InterruptedException {

        Cluster cluster = new ClusterBuilder().start();

        Injector injector = Guice.createInjector(new ServiceRecipe(cluster));

        final EventLoopGroup httpServerEventGroup = new NioEventLoopGroup();

        try {
            new HTTPServer(8888, new WebServer(injector, httpServerEventGroup).routeMatcher).run();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}


