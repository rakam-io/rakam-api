package org.rakam;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import org.rakam.analysis.AnalysisVerticle;
import org.rakam.analysis.model.AggregationRule;
import org.rakam.analysis.model.MetricAggregationRule;
import org.rakam.analysis.model.TimeSeriesAggregationRule;
import org.rakam.server.WebServer;
import org.rakam.util.SpanDateTime;
import org.rakam.worker.aggregation.AggregationLogic;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Future;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

import java.util.*;

/**
 * Created by buremba on 21/12/13.
 */
public class ServiceStarter extends Verticle {
    int cpuCore = Runtime.getRuntime().availableProcessors();
    String projectRoot = System.getProperty("user.dir");

    public void start(final Future<Void> startedResult) {

        /*
        final JsonObject obj = new JsonObject();
        obj.putBoolean("auto-redeploy", true);
        obj.putString("-cluster", "");
        obj.putString("-cluster-port", "5701");
        obj.putString("-cluster-host", "192.168.0.15");
        */

        fillTrackerPreAggregation();

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

        container.deployVerticle(AnalysisVerticle.class.getName(), 1, new AsyncResultHandler<String>() {
            public void handle(AsyncResult<String> deployResult) {
                if (deployResult.succeeded()) {
                    container.logger().info(String.format("Analyzer verticle started with %s processes.", 1));
                    startedResult.setResult(null);
                } else {
                    startedResult.setFailure(deployResult.cause());
                }
            }
        });

        JsonObject queue_config = new JsonObject();
        queue_config.putString("address", "aggregation.orderQueue");
        //queue_config.putNumber("process_timeout", 300000);

        container.deployModule("io.vertx~mod-work-queue~2.1.0-SNAPSHOT", queue_config, 1, new AsyncResultHandler<String>() {
            public void handle(AsyncResult<String> asyncResult) {
                if (asyncResult.succeeded()) {
                    container.deployWorkerVerticle(AggregationLogic.class.getName(), new JsonObject(), cpuCore, false, new AsyncResultHandler<String>() {
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

    public void fillTrackerPreAggregation() {

        /*
        Config cfg = null;
        try {
            cfg = new FileSystemXmlConfig(String.valueOf(Paths.get(System.getProperty("user.dir"), "config", "hazelcast.xml")));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(cfg);
        */
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName("analytics").setPassword("");
        HazelcastInstance instance =  HazelcastClient.newHazelcastClient(clientConfig);

        Map<String, List<AggregationRule>> aggregation_map = instance.getMap("aggregation.rules");


        List<AggregationRule> aggs = new ArrayList();
        aggs.add(new MetricAggregationRule(UUID.fromString("dd93140c-df8c-4813-bc40-b9bd8a805e90")));
        HashMap<String, String> a = new HashMap();
        a.put("a", "a");
        aggs.add(new MetricAggregationRule(UUID.fromString("e7460792-1dad-4803-b998-1e58c31e55c1"), a));
        aggs.add(new TimeSeriesAggregationRule(UUID.fromString("243bf77f-2a7c-4a6f-a60f-91ab31ec19d2"), SpanDateTime.fromPeriod("1m")));

        // tracker_id -> aggregation rules
        aggregation_map.put("e74607921dad4803b998", aggs);
    }

    public void stop() {

    }

}
