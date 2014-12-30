package org.rakam.analysis;

import com.google.inject.Injector;
import io.netty.handler.codec.http.HttpMethod;
import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.rakam.database.AnalysisRuleDatabase;
import org.rakam.database.DatabaseAdapter;
import org.rakam.server.RouteMatcher;
import org.rakam.util.json.JsonArray;
import org.rakam.util.json.JsonObject;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import static org.rakam.util.TimeUtil.UTCTime;

/**
 * Created by buremba on 07/05/14.
 */
public class AnalysisRuleCrudService implements HttpService {
    private static Logger LOGGER = Logger.getLogger("AnalysisRuleCrudHandler");
    private final AnalysisRuleMap analysisRuleMap;
    AnalysisRuleDatabase ruleDatabaseAdapter;
    DatabaseAdapter databaseAdapter;
    ExecutorService pool = Executors.newCachedThreadPool();

    public AnalysisRuleCrudService(Injector injector, AnalysisRuleMap analysisRuleMap) {
        ruleDatabaseAdapter = injector.getInstance(AnalysisRuleDatabase.class);
        databaseAdapter = injector.getInstance(DatabaseAdapter.class);
        this.analysisRuleMap = analysisRuleMap;
    }


    @Override
    public void register(RouteMatcher.MicroRouteMatcher routeMatcher) {
        routeMatcher.add("/add", HttpMethod.POST, request -> request.bodyHandler(obj -> {
            final JsonObject jsonObject = new JsonObject(obj);
            final JsonObject add = add(jsonObject);
            request.response(add.encode()).end();
        }));
        routeMatcher.add("/list", HttpMethod.POST, request -> request.bodyHandler(obj -> {
            final JsonObject jsonObject = new JsonObject(obj);
            final JsonObject add = list(jsonObject.getString("tracker"));
            request.response(add.encode()).end();
        }));
        routeMatcher.add("/get", HttpMethod.POST, request -> request.bodyHandler(obj -> {
            final JsonObject json = new JsonObject(obj);
            final JsonObject add = get(json.getString("tracker"), json.getString("rule"));
            request.response(add.encode()).end();
        }));
        // return new JsonObject().putString("error", "unknown endpoint. available endpoints: [add, list, delete, get]");

//        String action = obj.getString("action");
//        JsonObject request = obj.getObject("request");
//        String project = request.getString("tracker");
//        if (project == null) {
//            return new JsonObject().putString("error", "tracker parameter must be specified.").putNumber("status", 400);
//        }
    }

    private JsonObject delete(final JsonObject rule_obj) {
        AnalysisRule rule;
        JsonObject ret = new JsonObject();
        try {
            rule = AnalysisRuleParser.parse(rule_obj);
        } catch (IllegalArgumentException e) {
            ret.putString("error", e.getMessage());
            ret.putNumber("error_code", 400);
            return ret;
        }

        JsonObject request = new JsonObject()
                .putNumber("operation", AnalysisRuleMapActor.DELETE)
                .putString("tracker", rule.project).putObject("rule", rule.toJson())
                .putNumber("timestamp", UTCTime());

//        switch (rule.strategy) {
//            case REAL_TIME:
//                vertx.eventBus().publish("aggregationRuleReplication", request);
//                break;
//            case REAL_TIME_BATCH_CONCURRENT:
//                vertx.eventBus().publish("aggregationRuleReplication", request);
//                ruleDatabaseAdapter.deleteRule(rule);
//                break;
//            case BATCH:
//                ruleDatabaseAdapter.deleteRule(rule);
//                break;
//        }

        ret.putBoolean("success", true);
        return ret;
    }

    private JsonObject list(String project) {
        JsonObject ret = new JsonObject();
        Set<AnalysisRule> rules = analysisRuleMap.get(project);
        JsonArray json = new JsonArray();
        if (rules != null)
            for (AnalysisRule rule : rules) {
                json.add(rule.toJson());
            }
        ret.putArray("rules", json);
        return ret;
    }

    private JsonObject get(String project, String ruleId) {
        Set<AnalysisRule> rules = analysisRuleMap.get(project);
        if (rules != null)
            for (AnalysisRule rule : rules) {
                if (rule.id().equals(ruleId))
                    return rule.toJson();
            }
        else
            return new JsonObject().putString("error", "project doesn't exists");
        return new JsonObject().putString("error", "rule doesn't exists");
    }

    public JsonObject add(final JsonObject obj) {
        final AnalysisRule rule;
        JsonObject ret = new JsonObject();
        try {
            rule = AnalysisRuleParser.parse(obj);
        } catch (IllegalArgumentException e) {
            ret.putString("error", e.getMessage());
            ret.putNumber("status", 400);
            return ret;
        }
        Set<AnalysisRule> rules = analysisRuleMap.get(rule.project);
        if (rules != null && rules.contains(rule)) {
            ret.putString("error", "the rule already exists.");
            ret.putNumber("status", 400);
            return ret;
        }

        pool.submit(() -> {
            LOGGER.info("Processing the rule.");
            JsonObject request = new JsonObject()
                    .putNumber("operation", AnalysisRuleMapActor.ADD)
                    .putString("tracker", rule.project).putObject("rule", obj)
                    .putNumber("timestamp", UTCTime());
            ruleDatabaseAdapter.addRule(rule);
            switch (rule.strategy) {
                case REAL_TIME_BATCH_CONCURRENT:
//                    vertx.eventBus().publish(DistributedAnalysisRuleMap.IDENTIFIER, request);
                    databaseAdapter.processRule(rule);
                    updateBatchStatus(rule);
                    break;
                case BATCH:
                    databaseAdapter.processRule(rule);
                    updateBatchStatus(rule);
                    break;
                case REAL_TIME:
//                    vertx.eventBus().publish(DistributedAnalysisRuleMap.IDENTIFIER, request);
                    break;
                case BATCH_PERIODICALLY:
                    Integer batch_interval = obj.getInteger("batch_interval");
                    if (batch_interval == null) {
                        ret.putString("error", "batch_interval is required when analysis strategy is BATCH_PERIODICALLY");
                    }
//                vertx.setPeriodic(batch_interval, l -> databaseAdapter.processRule(rule));
                    break;
                default:
                    throw new IllegalStateException();
            }

            LOGGER.info("Rule is processed.");
        });
        ret.putString("message", "analysis rule successfully queued.");
        return ret;
    }

    private void updateBatchStatus(AnalysisRule rule) {
        rule.batch_status = true;
//        vertx.eventBus().publish("aggregationRuleReplication", new JsonObject()
//                .putNumber("operation", DistributedAnalysisRuleMap.UPDATE_BATCH)
//                .putString("tracker", rule.project).putObject("rule", rule.toJson())
//                .putNumber("timestamp", UTCTime()));
    }

    @Override
    public String getEndPoint() {
        return "/analysis_rule";
    }

}
