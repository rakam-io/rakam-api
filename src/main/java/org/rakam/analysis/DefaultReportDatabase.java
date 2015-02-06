package org.rakam.analysis;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.rakam.analysis.rule.aggregation.AggregationReport;
import org.rakam.database.ReportDatabase;
import org.rakam.kume.Cluster;
import org.rakam.kume.service.Service;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by buremba <Burak Emre Kabakcı> on 26/10/14 16:34.
 */
@Singleton
public class DefaultReportDatabase implements ReportDatabase {
    private final AnalysisRuleMapService map;

    @Inject
    public DefaultReportDatabase(Cluster cluster) {
        map = cluster.createOrGetService("ruleMap", bus -> new AnalysisRuleMapService(bus));
    }

    @Override
    public Map<String, Set<AggregationReport>> getAllReports() {
        return map.getAllRules();
    }

    @Override
    public void add(AggregationReport rule) {
        map.add(rule);
    }

    @Override
    public void delete(AggregationReport rule) {
        map.delete(rule);
    }

    @Override
    public Set<AggregationReport> get(String project) {
        Set<AggregationReport> aggregationReports = map.get(project);
        if(aggregationReports==null)
            return new HashSet<>();
        else
            return Collections.unmodifiableSet(aggregationReports);
    }

    @Override
    public void clear() {
        map.clear();
    }

    public static class AnalysisRuleMapService extends Service {
        private final Map<String, Set<AggregationReport>> map;
        private final Cluster.ServiceContext<AnalysisRuleMapService> ctx;

        public AnalysisRuleMapService(Cluster.ServiceContext<AnalysisRuleMapService> ctx) {
            this.ctx = ctx;
            this.map = new ConcurrentHashMap<>();
        }


        public synchronized void add(AggregationReport rule) {
            ctx.replicateSafely((service, ctx) ->
                    service.map.computeIfAbsent(rule.project, x ->
                            Collections.newSetFromMap(new ConcurrentHashMap<>())).add(rule));
        }

        public void delete(AggregationReport rule) {
            ctx.replicateSafely((service, ctx) -> {
                Set<AggregationReport> aggregationReports = service.map.get(rule.project);
                if(aggregationReports !=null)
                    aggregationReports.remove(rule);
            });
        }

        @Override
        public void onClose() {
            map.clear();
        }

        public Set<AggregationReport> get(String project) {
            return map.get(project);
        }

        public Set<Map.Entry<String, Set<AggregationReport>>> entrySet() {
            return map.entrySet();
        }

        public Collection<Set<AggregationReport>> values() {
            return map.values();
        }

        public Map<String, Set<AggregationReport>> getAllRules() {
            return Collections.unmodifiableMap(map);
        }

        public void clear() {
            ctx.replicateSafely((service, ctx) -> service.map.clear());
        }
    }
}
