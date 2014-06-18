package org.rakam;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.rakam.analysis.AnalysisRequestHandler;
import org.rakam.analysis.AnalysisRuleCrudHandler;
import org.rakam.cache.CacheAdapter;
import org.rakam.cache.DistributedAnalysisRuleMap;
import org.rakam.collection.CollectionWorker;
import org.rakam.collection.PeriodicCollector;
import org.rakam.database.DatabaseAdapter;
import org.rakam.server.WebServer;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.spi.cluster.NodeListener;
import org.vertx.java.platform.Verticle;
import org.vertx.java.platform.impl.DefaultPlatformManager;
import org.vertx.java.spi.cluster.impl.hazelcast.HazelcastClusterManager;

import java.util.Map;
import java.util.logging.Logger;

/**
 * Created by buremba on 21/12/13.
 */

public class ServiceStarter extends Verticle {
    int cpuCore = Runtime.getRuntime().availableProcessors();
    public static Injector injector;
    public static JsonObject conf;
    public static int server_id;
    public long collectionCollectorTimer;
    public static Logger logging = Logger.getLogger(ServiceStarter.class.getName());
    CacheAdapter cacheAdapter;

    public void start(final Future<Void> startedResult) {
        conf = container.config();
        JsonArray plugins = conf.getArray("plugins");
        injector = Guice.createInjector(new ServiceRecipe(plugins));
        cacheAdapter = ServiceStarter.injector.getInstance(CacheAdapter.class);
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

        org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.ERROR);

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

        collectionCollectorTimer = vertx.setPeriodic(2000, new Handler<Long>() {
            public void handle(Long timerID) {
                //long first = System.currentTimeMillis();
                for(Map.Entry item : DistributedAnalysisRuleMap.entrySet())
                    PeriodicCollector.process(item);
                //System.out.print(" "+(System.currentTimeMillis() - first)+" ");
            }
        });
        container.deployWorkerVerticle(CollectionWorker.class.getName());
        vertx.eventBus().registerHandler("aggregationRuleReplication", new DistributedAnalysisRuleMap());
        vertx.eventBus().registerHandler("analysisRequest", new AnalysisRequestHandler());
        vertx.eventBus().registerHandler("analysisRuleCrud", new AnalysisRuleCrudHandler(vertx.eventBus()));

        try {
            final DefaultPlatformManager mgr = (DefaultPlatformManager) FieldUtils.readField(container, "mgr", true);
            final HazelcastClusterManager cluster = (HazelcastClusterManager) FieldUtils.readField(mgr, "clusterManager", true);
            server_id = cluster.getID();
            cluster.nodeListener(new NodeListener() {
                @Override
                public void nodeAdded(String nodeID) {
                    logging.finest("say welcome to +"+nodeID+"!");
                }
                @Override
                public void nodeLeft(String nodeID) {
                    int intId = cluster.getNodeIntID(nodeID);
                    logging.fine("it seems "+intId+" is down. checking last check-in timestamp.");
                    long downTimestamp = cacheAdapter.getCounter(Integer.toString(intId));
                    logging.fine("node "+intId+" is down until "+downTimestamp);

                    DatabaseAdapter dbAdapter = injector.getInstance(DatabaseAdapter.class);
                    dbAdapter.batch((int) downTimestamp, intId);

                }
            });

        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

        startedResult.complete();
    }

    public void stop() {
        vertx.cancelTimer(collectionCollectorTimer);
    }

}
