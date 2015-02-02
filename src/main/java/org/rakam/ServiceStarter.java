package org.rakam;

import com.google.inject.Guice;
import com.google.inject.Injector;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.rakam.kume.Cluster;
import org.rakam.kume.ClusterBuilder;
import org.rakam.server.WebServer;

import java.io.FileNotFoundException;
import java.util.Properties;

/**
 * Created by buremba on 21/12/13.
 */

public class ServiceStarter {

    public static void main(String[] args) throws FileNotFoundException, InterruptedException {

//        Runtime.getRuntime().addShutdownHook(new Thread() {
//            @Override
//            public void run() {
//                System.out.println("exiting..");
//                TODO: gracefully exit.
//                System.exit(0);
//            }
//        });

        Cluster cluster = new ClusterBuilder().start();

        Properties props = new Properties();
        props.put("metadata.broker.list", "127.0.0.1:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        ProducerConfig config = new ProducerConfig(props);
        Producer producer = new Producer(config);

        Injector injector = Guice.createInjector(new ServiceRecipe(cluster, producer));

        try {
            injector.getInstance(WebServer.class).run(9999);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}


