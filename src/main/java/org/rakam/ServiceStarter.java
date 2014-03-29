package org.rakam;

import com.hazelcast.config.Config;
import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.rakam.analysis.AggregationRuleListFactory;
import org.rakam.analysis.AnalysisVerticle;
import org.rakam.analysis.model.AggregationRuleList;
import org.rakam.analysis.model.MetricAggregationRule;
import org.rakam.analysis.model.TimeSeriesAggregationRule;
import org.rakam.constant.AggregationType;
import org.rakam.server.WebServer;
import org.rakam.util.SpanDateTime;
import org.rakam.worker.aggregation.AggregationLogic;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Future;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

import java.io.FileNotFoundException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by buremba on 21/12/13.
 */
public class ServiceStarter extends Verticle {
    int cpuCore = Runtime.getRuntime().availableProcessors();
    String projectRoot = System.getProperty("user.dir");

    public void start(final Future<Void> startedResult) {

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


        Config cfg = null;
        try {
            cfg = new FileSystemXmlConfig(String.valueOf(Paths.get(System.getProperty("user.dir"), "config", "hazelcast.xml")));
            cfg.getSerializationConfig().addDataSerializableFactory(AggregationRuleListFactory.ID, new AggregationRuleListFactory());
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
        Map<String, AggregationRuleList> aggregation_map = instance.getMap("aggregation.rules");

        AggregationRuleList aggs = new AggregationRuleList();
        String projectId = "e74607921dad4803b998";
        //aggs.add(new MetricAggregationRule(projectId, AggregationType.COUNT_X, "test"));
        aggs.add(new MetricAggregationRule(projectId, AggregationType.SUM_X, "test"));
        aggs.add(new MetricAggregationRule(projectId, AggregationType.MAXIMUM_X, "test"));
        aggs.add(new TimeSeriesAggregationRule(projectId, AggregationType.SELECT_UNIQUE_Xs, SpanDateTime.fromPeriod("1min"), "referral", null, "referral"));

        HashMap<String, String> a = new HashMap();
        a.put("a", "a");
        aggs.add(new MetricAggregationRule(projectId, AggregationType.AVERAGE_X, "test", a));
        aggs.add(new TimeSeriesAggregationRule(projectId, AggregationType.COUNT_X, SpanDateTime.fromPeriod("1min"), "referral"));
        aggs.add(new TimeSeriesAggregationRule(projectId, AggregationType.COUNT, SpanDateTime.fromPeriod("1min"), null, null, "a"));

        // tracker_id -> aggregation rules
        aggregation_map.put("e74607921dad4803b998", aggs);
    }

    public void stop() {

    }

}
