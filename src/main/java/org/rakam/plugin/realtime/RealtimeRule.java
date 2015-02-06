package org.rakam.plugin.realtime;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.rakam.kume.Cluster;

import java.util.List;
import java.util.Map;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/02/15 18:13.
 */
public class RealtimeRule {
    public final String segment;
    public final List<String> measures;
    public Map<String, ObjectNode> db;

    public RealtimeRule(String segment, List<String> measures) {
        this.segment = segment;
        this.measures = measures;
    }

    public void createDatabase(Cluster cluster) {
//        cluster.createOrGetService();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RealtimeRule)) return false;

        RealtimeRule that = (RealtimeRule) o;

        if (!segment.equals(that.segment)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return segment.hashCode();
    }
}