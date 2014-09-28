package org.rakam.analysis;

import org.rakam.ServiceStarter;
import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.rakam.database.AnalysisRuleDatabase;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by buremba <Burak Emre Kabakcı> on 19/09/14 22:02.
 */
public class AnalysisRuleMap {
    final Map<String, HashSet<AnalysisRule>> map;

    public AnalysisRuleMap() {
        map = new ConcurrentHashMap(ServiceStarter.injector.getInstance(AnalysisRuleDatabase.class).getAllRules());

    }

    public HashSet<AnalysisRule> get(String project) {
        return map.get(project);
    }

    public Set<Map.Entry<String, HashSet<AnalysisRule>>> entrySet() {
        return map.entrySet();
    }

    public Set<String> keys() {
        return map.keySet();
    }

    public void add(String project, AnalysisRule rule) {
        map.computeIfAbsent(project, x -> new HashSet()).add(rule);
    }

    public void remove(String project, AnalysisRule rule) {
        map.computeIfPresent(project, (x, v) -> {
            v.remove(rule);
            return v;
        });
    }

    public void updateBatch(String project, AnalysisRule rule) {
        map.computeIfPresent(project, (x, v) -> {
            v.forEach(r -> {
                if(r.equals(r)) rule.batch_status = true;
            });
            return v;
        });
    }
}
