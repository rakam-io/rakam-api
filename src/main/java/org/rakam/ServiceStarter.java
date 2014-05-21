package org.rakam;

import com.hazelcast.config.Config;
import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.rakam.analysis.AnalysisRequestHandler;
import org.rakam.analysis.AnalysisRuleCrudHandler;
import org.rakam.collection.CollectionRequestHandler;
import org.rakam.server.WebServer;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Future;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

import java.io.FileNotFoundException;
import java.nio.file.Paths;

/**
 * Created by buremba on 21/12/13.
 */

public class ServiceStarter extends Verticle {
    int cpuCore = Runtime.getRuntime().availableProcessors();
    String projectRoot = System.getProperty("user.dir");

    public void start(final Future<Void> startedResult) {
        /*
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        //replace "hive" here with the name of the user the queries should extract as
        Connection con = null;

        try {
            con = DriverManager.getConnection("jdbc:hive2://localhost:10000/src", "buremba", "ooop");
            Statement stmt = con.createStatement();
            stmt.execute("create table event (key int, value string)");
            ResultSet res = stmt.executeQuery("show tables");
            if (res.next()) {
                container.logger().debug(res.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        */
        //Logger.getGlobal().setLevel(Level.WARNING);

        startHazelcast();

        //org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.ERROR);

        container.deployVerticle(WebServer.class.getName(), cpuCore, new AsyncResultHandler<String>() {
            public void handle(AsyncResult<String> deployResult) {
                if (deployResult.succeeded()) {
                    container.logger().info(String.format("Webserver started with %s processes.", cpuCore));
                    startedResult.setResult(null);
                } else {
                    startedResult.setFailure(deployResult.cause());
                }
            }
        });

        vertx.eventBus().registerHandler("analysisRequest", new AnalysisRequestHandler());
        vertx.eventBus().registerHandler("analysisRuleCrud", new AnalysisRuleCrudHandler());

        JsonObject queue_config = new JsonObject();
        queue_config.putString("address", "request.orderQueue");
        //queue_config.putNumber("process_timeout", 300000);
        container.deployModule("io.vertx~mod-work-queue~2.1.0-SNAPSHOT", queue_config, 1, new AsyncResultHandler<String>() {
            public void handle(AsyncResult<String> asyncResult) {
                if (asyncResult.succeeded()) {
                    container.deployWorkerVerticle(CollectionRequestHandler.class.getName(), new JsonObject(), cpuCore, false, new AsyncResultHandler<String>() {
                        @Override
                        public void handle(AsyncResult<String> asyncResult1) {
                            if (asyncResult1.failed())
                                asyncResult1.cause().printStackTrace();

                        }
                    });
                } else {
                    asyncResult.cause().printStackTrace();
                }
            }
        });
    }

    public void startHazelcast() {
        Config cfg = null;
        try {
            cfg = new FileSystemXmlConfig(String.valueOf(Paths.get(System.getProperty("user.dir"), "config", "hazelcast.xml")));
            //cfg.getSerializationConfig().addDataSerializableFactory(AggregationRuleListFactory.ID, new AggregationRuleListFactory());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(cfg);
        /*
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName("analytics").setPassword("");
        HazelcastInstance instance =  HazelcastClient.newHazelcastClient(clientConfig);
        */
    }

    public void stop() {

    }

}
