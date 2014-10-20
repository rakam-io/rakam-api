package org.rakam.analysis;

import org.rakam.ServiceStarter;
import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.rakam.cache.DistributedAnalysisRuleMap;
import org.rakam.database.AnalysisRuleDatabase;
import org.rakam.database.DatabaseAdapter;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import static org.rakam.util.DateUtil.UTCTime;

/**
 * Created by buremba on 07/05/14.
 */
public class AnalysisRuleCrudHandler implements Handler<Message<JsonObject>> {
    private static Logger LOGGER = Logger.getLogger("AnalysisRuleCrudHandler");
    public static final String IDENTIFIER = "analysisRuleCrud";

    private final Vertx vertx;

    AnalysisRuleDatabase ruleDatabaseAdapter = ServiceStarter.injector.getInstance(AnalysisRuleDatabase.class);
    DatabaseAdapter databaseAdapter = ServiceStarter.injector.getInstance(DatabaseAdapter.class);
    ExecutorService pool = Executors.newCachedThreadPool();

    public AnalysisRuleCrudHandler(Vertx vertx) {
        this.vertx = vertx;
    }


    @Override
    public void handle(Message<JsonObject> event) {
        final JsonObject obj = event.body();
        String action = obj.getString("action");
        JsonObject request = obj.getObject("request");
        String project = request.getString("tracking");
        if (project == null) {
            event.reply(new JsonObject().putString("error", "tracking parameter must be specified.").putNumber("status", 400));
            return;
        }

        if (action.equals("add"))
            event.reply(add(request));
        else if(action.equals("list"))
            event.reply(list(request.getString("tracking")));
        else if(action.equals("delete"))
            event.reply(delete(request.getObject("rule")));
        else if(action.equals("get"))
            event.reply(get(request.getString("tracking"), request.getString("rule")));
        else
            event.reply(new JsonObject().putString("error", "unknown endpoint. available endpoints: [add, list, delete, get]"));
    }

    private JsonObject delete(final JsonObject rule_obj) {
        AnalysisRule rule;
        JsonObject ret = new JsonObject();
        try {
            rule = AnalysisRuleParser.parse(rule_obj);
        } catch(IllegalArgumentException e) {
            ret.putString("error", e.getMessage());
            ret.putNumber("error_code", 400);
            return ret;
        }

        JsonObject request = new JsonObject()
                .putNumber("operation", DistributedAnalysisRuleMap.DELETE)
                .putString("tracking", rule.project).putObject("rule", rule.toJson())
                .putNumber("timestamp", UTCTime());

        switch (rule.strategy) {
            case REAL_TIME:
                vertx.eventBus().publish("aggregationRuleReplication", request);
                break;
            case REAL_TIME_BATCH_CONCURRENT:
                vertx.eventBus().publish("aggregationRuleReplication", request);
                ruleDatabaseAdapter.deleteRule(rule);
                break;
            case BATCH:
                ruleDatabaseAdapter.deleteRule(rule);
                break;
        }

        ret.putBoolean("success", true);
        return ret;
    }

    private JsonObject list(String project) {
        JsonObject ret = new JsonObject();
        Set<AnalysisRule> rules = DistributedAnalysisRuleMap.get(project);
        JsonArray json = new JsonArray();
        if(rules!=null)
            for(AnalysisRule rule : rules) {
                json.add(rule.toJson());
            }
        ret.putArray("rules", json);
        return ret;
    }

    private JsonObject get(String project, String ruleId) {
        Set<AnalysisRule> rules = DistributedAnalysisRuleMap.get(project);
        if(rules!=null)
            for(AnalysisRule rule : rules) {
                if(rule.id().equals(ruleId))
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
        } catch(IllegalArgumentException e) {
            ret.putString("error", e.getMessage());
            ret.putNumber("status", 400);
            return ret;
        }
        Set<AnalysisRule> rules = DistributedAnalysisRuleMap.get(rule.project);
        if(rules !=null && rules.contains(rule)) {
            ret.putString("error", "the rule already exists.");
            ret.putNumber("status", 400);
            return ret;
        }

        pool.submit(() -> {
            LOGGER.info("Processing the rule.");
            JsonObject request = new JsonObject()
                    .putNumber("operation", DistributedAnalysisRuleMap.ADD)
                    .putString("tracking", rule.project).putObject("rule", obj)
                    .putNumber("timestamp", UTCTime());
            ruleDatabaseAdapter.addRule(rule);
            switch (rule.strategy) {
                case REAL_TIME_BATCH_CONCURRENT:
                    vertx.eventBus().publish(DistributedAnalysisRuleMap.IDENTIFIER, request);
                    databaseAdapter.processRule(rule);
                    updateBatchStatus(rule);
                    break;
                case BATCH:
                    databaseAdapter.processRule(rule);
                    updateBatchStatus(rule);
                    break;
                case REAL_TIME:
                    vertx.eventBus().publish(DistributedAnalysisRuleMap.IDENTIFIER, request);
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
        ret.putString("status", "analysis rule successfully queued.");
        return ret;
    }

    private void updateBatchStatus(AnalysisRule rule) {
        rule.batch_status = true;
        vertx.eventBus().publish("aggregationRuleReplication", new JsonObject()
                .putNumber("operation", DistributedAnalysisRuleMap.UPDATE_BATCH)
                .putString("tracking", rule.project).putObject("rule", rule.toJson())
                .putNumber("timestamp", UTCTime()));
    }
}
