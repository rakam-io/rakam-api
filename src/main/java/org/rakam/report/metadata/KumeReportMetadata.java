package org.rakam.report.metadata;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.rakam.analysis.rule.aggregation.AggregationReport;
import org.rakam.kume.Cluster;
import org.rakam.kume.service.Service;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by buremba <Burak Emre Kabakcı> on 26/10/14 16:34.
 */
@Singleton
public class KumeReportMetadata implements ReportMetadataStore {
    private final AnalysisRuleMapService map;

    @Inject
    public KumeReportMetadata(Cluster cluster) {
        map = cluster.createOrGetService("ruleMap", bus -> new AnalysisRuleMapService(bus));
    }


    @Override
    public void createReport(String project, String name, String query) {

    }

    @Override
    public void deleteReport(String project, String name) {

    }

    @Override
    public String getReport(String project, String name) {
        return null;
    }

    @Override
    public Map<String, String> getReports(String project) {
        return null;
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
