package org.rakam;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.HazelcastInstanceProxy;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.rakam.analysis.AnalysisRequestHandler;
import org.rakam.analysis.AnalysisRuleCrudHandler;
import org.rakam.analysis.FilterRequestHandler;
import org.rakam.cache.CacheAdapter;
import org.rakam.cache.DistributedAnalysisRuleMap;
import org.rakam.collection.CollectionWorker;
import org.rakam.collection.PeriodicCollector;
import org.rakam.database.DatabaseAdapter;
import org.rakam.server.WebServer;
import org.vertx.java.core.Future;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.spi.cluster.ClusterManager;
import org.vertx.java.platform.Verticle;
import org.vertx.java.platform.impl.DefaultPlatformManager;

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
    private HazelcastInstanceProxy hazelcast;

    public void start(final Future<Void> startedResult) {
        conf = container.config();
        JsonArray plugins = conf.getArray("plugins");
        injector = Guice.createInjector(new ServiceRecipe(plugins));
        cacheAdapter = ServiceStarter.injector.getInstance(CacheAdapter.class);
//        org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.ERROR);

        container.deployVerticle(WebServer.class.getName(), cpuCore, deployResult -> {
            if (deployResult.succeeded()) {
                container.logger().info(String.format("Webserver started with %s processes.", cpuCore));
                startedResult.setResult(null);
            } else {
                startedResult.setFailure(deployResult.cause());
            }
        });

        collectionCollectorTimer = vertx.setPeriodic(2000, timerID -> {
            //long first = System.currentTimeMillis();
            DistributedAnalysisRuleMap.entrySet().forEach(PeriodicCollector::process);
            //System.out.print(" "+(System.currentTimeMillis() - first)+" ");
        });

        container.deployWorkerVerticle(CollectionWorker.class.getName());
        vertx.eventBus().registerHandler("aggregationRuleReplication", new DistributedAnalysisRuleMap());
        vertx.eventBus().registerHandler("analysisRequest", new AnalysisRequestHandler());

        FilterRequestHandler handler = new FilterRequestHandler();
        vertx.eventBus().registerHandler("eventFilterRequest", handler);
        vertx.eventBus().registerHandler("actorFilterRequest", handler);

        vertx.eventBus().registerHandler("analysisRuleCrud", new AnalysisRuleCrudHandler(vertx));

        try {
            final DefaultPlatformManager mgr = (DefaultPlatformManager) FieldUtils.readField(container, "mgr", true);
            final ClusterManager cluster = (ClusterManager) FieldUtils.readField(mgr, "clusterManager", true);
            this.hazelcast = (HazelcastInstanceProxy) FieldUtils.readField(cluster, "hazelcast", true);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

        server_id = (int) hazelcast.getAtomicLong("server_id").incrementAndGet();
        hazelcast.getCluster().addMembershipListener(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                String nodeID = membershipEvent.getMember().getUuid();
                logging.finest("say welcome to +" + nodeID + "!");
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                String nodeID = membershipEvent.getMember().getUuid();
                logging.fine("it seems " + nodeID + " is down. checking last check-in timestamp.");
                long downTimestamp = cacheAdapter.getCounter(nodeID);
                logging.fine("node " + nodeID + " is down until " + downTimestamp);

                DatabaseAdapter dbAdapter = injector.getInstance(DatabaseAdapter.class);
//                dbAdapter.batch((int) downTimestamp, server_id);
            }

            @Override
            public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {

            }
        });

        startedResult.complete();
    }

    public void stop() {
        vertx.cancelTimer(collectionCollectorTimer);
    }

}


