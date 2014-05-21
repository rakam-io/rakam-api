package org.rakam.analysis;

import com.hazelcast.core.IMap;
import database.cassandra.CassandraBatchProcessor;
import org.rakam.analysis.rule.AnalysisRule;
import org.rakam.analysis.rule.AnalysisRuleList;
import org.rakam.cache.CacheAdapterFactory;
import org.rakam.cache.SimpleCacheAdapter;
import org.rakam.cache.hazelcast.HazelcastCacheAdapter;
import org.rakam.database.cassandra.CassandraAdapter;
import org.vertx.java.core.Handler;
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

    CassandraAdapter databaseAdapter = new CassandraAdapter();
    SimpleCacheAdapter cacheAdapter = new HazelcastCacheAdapter();
    IMap<String, AnalysisRuleList> cacheAggs = CacheAdapterFactory.getAggregationMap();
    ExecutorService pool = Executors.newCachedThreadPool();


    @Override
    public void handle(Message<JsonObject> event) {
        final JsonObject obj = event.body();
        String action = obj.getString("_action");
        if (action.equals("add"))
            event.reply(add(obj.getObject("rule")));
        else if(action.equals("list"))
            event.reply(list(obj.getString("project")));
    }

    private JsonObject list(String project) {
        JsonObject ret = new JsonObject();
        AnalysisRuleList rules = cacheAggs.get(project);
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
        obj.removeField("project");
        AnalysisRuleList list = cacheAggs.get(rule.project);
        if(list==null)
            list = new AnalysisRuleList();
        if(!list.add(rule)) {
            ret.putString("error", "the rule already exists.");
            ret.putNumber("error_code", 200);
            return ret;
        }

        cacheAggs.put(rule.project, list);


        final AnalysisRuleList finalList = list;
        pool.submit(new Runnable() {
            @Override
            public void run() {
                databaseAdapter.addRule(rule.project, obj.encode());
                LOGGER.info("Running Hadoop Job on Spark..");
                CassandraBatchProcessor.processRule(rule);
                LOGGER.info("Hadoop Job executed on Spark.");

                LOGGER.info("Exporting data from database to cache.");
                try {
                    CassandraBatchProcessor.exportCurrentToCache(cacheAdapter, rule);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                rule.batch_status = true;
                cacheAggs.put(rule.project, finalList);
                LOGGER.info("Rule is processed.");
            }
        });
        ret.putString("status", "analysis rule successfully saved.");
        return ret;
    }
}
