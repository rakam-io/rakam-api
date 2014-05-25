package org.rakam.cache;

import org.rakam.ServiceStarter;
import org.rakam.analysis.AnalysisQueryParser;
import org.rakam.analysis.rule.AnalysisRuleList;
import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.rakam.database.DatabaseAdapter;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by buremba on 22/12/13.
 */
public class DistributedAnalysisRuleMap implements Handler<Message<JsonObject>> {
    final static Map<String, AnalysisRuleList> map;
    static {
        map = new ConcurrentHashMap(ServiceStarter.injector.getInstance(DatabaseAdapter.class).getAllRules());
    }

    public final static int ADD = 0;
    public final static int DELETE = 1;
    public final static int UPDATE = 2;

    public static AnalysisRuleList get(String project) {
        return map.get(project);
    }

    public static Set<Map.Entry<String, AnalysisRuleList>> entrySet() {
        return map.entrySet();
    }

    // check operation timestamp and compare the current version
    // because the request may be processed unordered and it may cause data loss.
    @Override
    public void handle(Message<JsonObject> message) {
        JsonObject json = message.body();
        String project = json.getString("project");
        AnalysisRuleList rules = map.get(project);
        if(rules==null) {
            rules = new AnalysisRuleList();
            map.put(project, rules);
        }
        if (json.getInteger("operation") == ADD) {
            rules.add(AnalysisQueryParser.parse(json.getObject("rule")));
        } else if (json.getInteger("operation") == DELETE) {
            rules.remove(AnalysisQueryParser.parse(json.getObject("rule")));
        } else if (json.getInteger("operation") == UPDATE) {
            AnalysisRule base = AnalysisQueryParser.parse(json.getObject("old_rule"));
            AnalysisRule new_rule = AnalysisQueryParser.parse(json.getObject("new_rule"));
            if (rules.contains(base)) {
                rules.remove(base);
                rules.add(new_rule);
            } else {
                throw new IllegalArgumentException("rule doesn't exist");
            }
        }else
            throw new IllegalArgumentException("operation doesn't exist");
    }
}
