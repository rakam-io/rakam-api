package org.rakam.plugin.realtime;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.rakam.kume.Cluster;
import org.rakam.kume.service.Service;
import org.rakam.kume.service.ringmap.RingMap;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/02/15 17:04.
 */
public class ConfigService extends Service {
    Cluster.ServiceContext<ConfigService> cluster;
    Map<String, Set<RealtimeRule>> rules;

    public ConfigService(Cluster.ServiceContext<ConfigService> ctx) {
        this.cluster = ctx;
    }

    public void addRule(String project, RealtimeRule rule) {
        cluster.replicateSafely((service, ctx) -> {
            Cluster cluster1 = service.cluster.getCluster();
            RingMap<String, ObjectNode> map = cluster1.createOrGetService(project+"_"+rule.segment, bus -> new RingMap<>(bus, (first, second) -> {
//                    first.setAll(second);
                return first;
            }, 1));
            rule.createDatabase(cluster1);
            service.rules.computeIfAbsent(project, key -> new HashSet()).add(rule);
        });
    }

    public Set<RealtimeRule> getRules(String project) {
        return rules.get(project);
    }

    @Override
    public void onClose() {

    }
}
