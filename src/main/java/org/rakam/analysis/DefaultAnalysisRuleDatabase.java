package org.rakam.analysis;

import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.rakam.database.AnalysisRuleDatabase;
import org.rakam.kume.Cluster;
import org.rakam.kume.service.Service;
import org.rakam.server.RouteMatcher;
import org.rakam.server.http.HttpService;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by buremba <Burak Emre Kabakcı> on 26/10/14 16:34.
 */
public class DefaultAnalysisRuleDatabase implements HttpService, AnalysisRuleDatabase {
    private final AnalysisRuleMapService map;

    public DefaultAnalysisRuleDatabase(Cluster cluster) {
        map = cluster.createOrGetService("ruleMap", AnalysisRuleMapService::new);
    }

    @Override
    public String getEndPoint() {
        return "/stream";
    }

    @Override
    public void register(RouteMatcher.MicroRouteMatcher routeMatcher) {

    }

    @Override
    public Map<String, Set<AnalysisRule>> getAllRules() {
        return map.getAllRules();
    }

    @Override
    public void add(AnalysisRule rule) {
        map.add(rule);
    }

    @Override
    public void delete(AnalysisRule rule) {
        map.delete(rule);
    }

    @Override
    public Set<AnalysisRule> get(String project) {
        return map.get(project);
    }

    @Override
    public void clear() {
        map.clear();
    }

    public static class AnalysisRuleMapService extends Service {
        private final Map<String, Set<AnalysisRule>> map;
        private final Cluster.ServiceContext<AnalysisRuleMapService> ctx;

        public AnalysisRuleMapService(Cluster.ServiceContext<AnalysisRuleMapService> ctx) {
            this.ctx = ctx;
            this.map = new ConcurrentHashMap<>();
        }


        public synchronized void add(AnalysisRule rule) {
            ctx.replicateSafely((service, ctx) ->
                    service.map.computeIfAbsent(rule.project, x -> {
                        Set<AnalysisRule> analysisRules = Collections.newSetFromMap(new ConcurrentHashMap<>());
                        return analysisRules;
                    }).add(rule));
        }

        public void delete(AnalysisRule rule) {
            ctx.replicateSafely((service, ctx) -> {
                Set<AnalysisRule> analysisRules = service.map.get(rule.project);
                if(analysisRules!=null)
                    analysisRules.remove(rule);
            });
        }

        @Override
        public void onClose() {
            map.clear();
        }

        public Set<AnalysisRule> get(String project) {
            return map.get(project);
        }

        public Set<Map.Entry<String, Set<AnalysisRule>>> entrySet() {
            return map.entrySet();
        }

        public Collection<Set<AnalysisRule>> values() {
            return map.values();
        }

        public Map<String, Set<AnalysisRule>> getAllRules() {
            return null;
        }

        public void clear() {
            ctx.replicateSafely((service, ctx) -> service.map.clear());
        }
    }
}
