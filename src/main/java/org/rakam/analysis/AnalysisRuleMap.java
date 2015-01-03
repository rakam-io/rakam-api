package org.rakam.analysis;

import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.rakam.util.json.JsonObject;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.StampedLock;

/**
 * Created by buremba <Burak Emre Kabakcı> on 26/10/14 16:34.
 */
public class AnalysisRuleMap {
    final private static StampedLock stampedLock = new StampedLock();
    private final Map<String, Set<AnalysisRule>> map;

    public AnalysisRuleMap() {
        map = new ConcurrentHashMap<>();
    }

    public AnalysisRuleMap(Map<String, Set<AnalysisRule>> rules) {
        map = rules;
    }

    public Set<AnalysisRule> get(String project) {
        if (stampedLock.tryOptimisticRead() == 0) {
            final long l = stampedLock.readLock();
            try {
                return map.get(project);
            } finally {
                stampedLock.unlockRead(l);
            }
        }
        return map.get(project);
    }

    public Set<Map.Entry<String, Set<AnalysisRule>>> entrySet() {
        return map.entrySet();
    }

    public Collection<Set<AnalysisRule>> values() {
        return map.values();
    }

    public Set<String> keys() {
        return map.keySet();
    }


    public synchronized void merge(Map<String, Set<AnalysisRule>> rules) {
        rules.forEach((k, v) -> map.put(k, v));
    }

    public synchronized void add(String project, AnalysisRule rule) {
        map.computeIfAbsent(project, x -> Collections.newSetFromMap(new ConcurrentHashMap<>())).add(rule);
    }

    public synchronized void remove(String project, AnalysisRule rule) {
        map.computeIfPresent(project, (x, v) -> {
            v.remove(rule);
            return v;
        });
    }

    protected synchronized void updateBatch(String project, AnalysisRule rule) {
        map.computeIfPresent(project, (x, v) -> {
            v.forEach(r -> {
                if (r.equals(r)) rule.batch_status = true;
            });
            return v;
        });
    }

    public void clear() {
        map.clear();
    }


    public static class AnalysisRuleMapActor {
        public final static int ADD = 0;
        public final static int DELETE = 1;
        public final static int UPDATE_BATCH = 2;
        public static final String TOPIC = "analysisRule";

        private final AnalysisRuleMap map;

        public AnalysisRuleMapActor(AnalysisRuleMap map) {
            this.map = map;
        }


        public void handle(JsonObject json) {
            String project = json.getString("tracker");

            switch (json.getInteger("operation")) {
                case ADD:
//                    map.add(project, AnalysisRuleParser.parse(json.getJsonObject("rule")));
                    break;
                case DELETE:
//                    map.remove(project, AnalysisRuleParser.parse(json.getJsonObject("rule")));
                case UPDATE_BATCH:
//                    map.updateBatch(project, AnalysisRuleParser.parse(json.getJsonObject("rule")));
                default:
                    throw new IllegalArgumentException("operation doesn't exist");
            }
        }
    }

}
