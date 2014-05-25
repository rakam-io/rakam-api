package org.rakam.analysis;

import database.cassandra.CassandraBatchProcessor;
import org.rakam.ServiceStarter;
import org.rakam.analysis.rule.AnalysisRuleList;
import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.rakam.cache.DistributedAnalysisRuleMap;
import org.rakam.constant.AnalysisRuleStrategy;
import org.rakam.database.DatabaseAdapter;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

/**
 * Created by buremba on 07/05/14.
 */
public class AnalysisRuleCrudHandler implements Handler<Message<JsonObject>> {
    private static Logger LOGGER = Logger.getLogger("AnalysisRuleCrudHandler");
    private final EventBus eventBus;

    DatabaseAdapter databaseAdapter = ServiceStarter.injector.getInstance(DatabaseAdapter.class);
    ExecutorService pool = Executors.newCachedThreadPool();

    public AnalysisRuleCrudHandler(EventBus eventBus) {
        this.eventBus = eventBus;
    }


    @Override
    public void handle(Message<JsonObject> event) {
        final JsonObject obj = event.body();
        String action = obj.getString("_action");
        if (action.equals("add"))
            event.reply(add(obj.getObject("rule")));
        else if(action.equals("list"))
            event.reply(list(obj.getString("project")));
        else if(action.equals("delete"))
            event.reply(delete(obj.getObject("rule")));
    }

    private JsonObject delete(final JsonObject rule_obj) {
        AnalysisRule rule;
        JsonObject ret = new JsonObject();
        try {
            rule = AnalysisQueryParser.parse(rule_obj);
        } catch(IllegalArgumentException e) {
            ret.putString("error", e.getMessage());
            ret.putNumber("error_code", 400);
            return ret;
        }

        databaseAdapter.deleteRule(rule);

        ret.putBoolean("success", true);
        return ret;
    }

    private JsonObject list(String project) {
        JsonObject ret = new JsonObject();
        AnalysisRuleList rules = DistributedAnalysisRuleMap.get(project);
        JsonArray json = new JsonArray();
        if(rules!=null)
            for(AnalysisRule rule : rules) {
                json.add(rule.toJson());
            }
        ret.putArray("rules", json);
        return ret;
    }

    public JsonObject add(final JsonObject obj) {
        final AnalysisRule rule;
        JsonObject ret = new JsonObject();
        try {
            rule = AnalysisQueryParser.parse(obj);
        } catch(IllegalArgumentException e) {
            ret.putString("error", e.getMessage());
            ret.putNumber("error_code", 400);
            return ret;
        }
        AnalysisRuleList list = DistributedAnalysisRuleMap.get(rule.project);
        if(list==null)
            list = new AnalysisRuleList();
        if(!list.add(rule)) {
            ret.putString("error", "the rule already exists.");
            ret.putNumber("error_code", 200);
            return ret;
        }

        pool.submit(new Runnable() {
            @Override
            public void run() {
                LOGGER.info("Processing the rule.");
                JsonObject request = new JsonObject().putNumber("operation", DistributedAnalysisRuleMap.ADD).putString("project", rule.project).putObject("rule", obj);
                databaseAdapter.addRule(rule);
                if(rule.strategy == AnalysisRuleStrategy.REAL_TIME_BATCH_CONCURRENT) {
                    eventBus.publish("aggregationRuleReplication", request);
                    CassandraBatchProcessor.processRule(rule);
                    rule.batch_status = true;
                }else
                if(rule.strategy == AnalysisRuleStrategy.REAL_TIME_AFTER_BATCH) {
                    CassandraBatchProcessor.processRule(rule);
                    eventBus.publish("aggregationRuleReplication", request);
                    rule.batch_status = true;
                }else
                if(rule.strategy == AnalysisRuleStrategy.BATCH) {
                    CassandraBatchProcessor.processRule(rule);
                    rule.batch_status = true;
                } else
                if(rule.strategy == AnalysisRuleStrategy.REAL_TIME)
                    eventBus.publish("aggregationRuleReplication", request);

                LOGGER.info("Rule is processed.");
            }
        });
        ret.putString("status", "analysis rule successfully queued.");
        return ret;
    }
}
