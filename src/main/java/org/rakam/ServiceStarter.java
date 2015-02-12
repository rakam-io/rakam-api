package org.rakam;

import com.google.common.base.Throwables;
import com.google.inject.Injector;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.rakam.collection.CollectionModule;
import org.rakam.kume.Cluster;
import org.rakam.kume.ClusterBuilder;
import org.rakam.report.metadata.ReportMetadataModule;
import org.rakam.server.WebServer;
import org.rakam.util.bootstrap.Bootstrap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by buremba on 21/12/13.
 */

public class ServiceStarter {
    final static Logger LOGGER = LoggerFactory.getLogger(Cluster.class);

    public static void main(String[] args) throws Throwable {

//        Runtime.getRuntime().addShutdownHook(new Thread() {
//            @Override
//            public void run() {
//                System.out.println("exiting..");
//                TODO: gracefully exit.
//                System.exit(0);
//            }
//        });

//        String main_dir = System.getProperty("user.dir");
//        File config_file = new File(main_dir, "config.properties");
//
//        Properties properties = new Properties();
//        properties.load(new FileInputStream(config_file));

        if (args.length > 0) {
            System.setProperty("config", args[0]);
        } else {
            System.setProperty("config", "config.properties");
        }

        Cluster cluster = new ClusterBuilder().start();

        Properties props = new Properties();
        props.put("metadata.broker.list", "127.0.0.1:9092");
        props.put("serializer.class", "kafka.serializer.DefaultEncoder");

        ProducerConfig config = new ProducerConfig(props);
        Producer producer = new Producer(config);

        Bootstrap app = new Bootstrap(
                new CollectionModule(),
                new ReportMetadataModule(),
                new ServiceRecipe(cluster, producer));
        app.requireExplicitBindings(false);
        try {
            Injector injector = app.strictConfig().initialize();
            injector.getInstance(WebServer.class).run(9999);
        }
        catch (Throwable e) {
            LOGGER.error("Error while starting app", e);
            Throwables.propagate(e);
            System.exit(1);
        }
    }
}