package org.rakam.cache;

import org.rakam.ServiceStarter;
import org.rakam.analysis.AnalysisRuleParser;
import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.rakam.database.AnalysisRuleDatabase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by buremba on 22/12/13.
 */
public class DistributedAnalysisRuleMap implements Handler<Message<JsonObject>> {
    final static Map<String, HashSet<AnalysisRule>> map;
    static {
        map = new ConcurrentHashMap(ServiceStarter.injector.getInstance(AnalysisRuleDatabase.class).getAllRules());
    }

    public final static int ADD = 0;
    public final static int DELETE = 1;
    public final static int UPDATE = 2;
    private static long timestampCursor = System.currentTimeMillis();

    public static HashSet<AnalysisRule> get(String project) {
        return map.get(project);
    }

    public static Set<Map.Entry<String, HashSet<AnalysisRule>>> entrySet() {
        return map.entrySet();
    }

    public static Set<String> keys() {
        return map.keySet();
    }

    // check operation timestamp and compare the current version
    // because the request may be processed unordered and it may cause data loss.
    @Override
    public void handle(Message<JsonObject> message) {
        JsonObject json = message.body();
        String project = json.getString("_tracking");
        Long timestamp = json.getLong("timestamp");
        if(timestamp!=null && timestamp<timestampCursor)
            throw new IllegalArgumentException("timestamp for event must be provided");
        timestampCursor = timestamp;
        HashSet<AnalysisRule> rules = map.get(project);
        if(rules==null) {
            rules = new HashSet();
            map.put(project, rules);
        }
        if (json.getInteger("operation") == ADD) {
            rules.add(AnalysisRuleParser.parse(json.getObject("rule")));
        } else if (json.getInteger("operation") == DELETE) {
            rules.remove(AnalysisRuleParser.parse(json.getObject("rule")));
        } else if (json.getInteger("operation") == UPDATE) {
            // The API level doesn't support UPDATE request so this code is unnecessary currently
            AnalysisRule base = AnalysisRuleParser.parse(json.getObject("old_rule"));
            AnalysisRule new_rule = AnalysisRuleParser.parse(json.getObject("new_rule"));
            if (rules.remove(base)) {
                rules.add(new_rule);
            } else {
                throw new IllegalArgumentException("rule doesn't exist");
            }
        }else
            throw new IllegalArgumentException("operation doesn't exist");
    }
}
