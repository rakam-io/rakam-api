package org.rakam.report.metadata;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.rakam.analysis.MaterializedView;
import org.rakam.analysis.Report;
import org.rakam.analysis.TableStrategy;
import org.rakam.kume.Cluster;
import org.rakam.kume.service.Service;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
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
    public void saveReport(Report report) {

    }

    @Override
    public void createMaterializedView(MaterializedView report) {

    }

    @Override
    public void deleteReport(String project, String name) {

    }

    @Override
    public Report getReport(String project, String name) {
        return null;
    }

    @Override
    public List<Report> getReports(String project) {
        return null;
    }

    @Override
    public Map<String, List<MaterializedView>> getAllMaterializedViews(TableStrategy strategy) {
        return null;
    }

    @Override
    public void updateMaterializedView(String project, String viewName, Instant lastUpdate) {

    }

    public static class AnalysisRuleMapService extends Service {
        private final Map<String, Set<String>> map;
        private final Cluster.ServiceContext<AnalysisRuleMapService> ctx;

        public AnalysisRuleMapService(Cluster.ServiceContext<AnalysisRuleMapService> ctx) {
            this.ctx = ctx;
            this.map = new ConcurrentHashMap<>();
        }


        public synchronized void add(String rule) {
            ctx.replicateSafely((service, ctx) ->
                    service.map.computeIfAbsent(rule, x ->
                            Collections.newSetFromMap(new ConcurrentHashMap<>())).add(rule));
        }

        public void delete(String rule) {
            ctx.replicateSafely((service, ctx) -> {
                Set<String> aggregationReports = service.map.get(rule);
                if(aggregationReports !=null)
                    aggregationReports.remove(rule);
            });
        }

        @Override
        public void onClose() {
            map.clear();
        }

        public Set<String> get(String project) {
            return map.get(project);
        }

        public Set<Map.Entry<String, Set<String>>> entrySet() {
            return map.entrySet();
        }

        public Collection<Set<String>> values() {
            return map.values();
        }

        public Map<String, Set<String>> getAllRules() {
            return Collections.unmodifiableMap(map);
        }

        public void clear() {
            ctx.replicateSafely((service, ctx) -> service.map.clear());
        }
    }
}
