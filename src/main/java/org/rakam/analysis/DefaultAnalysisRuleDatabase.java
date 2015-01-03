package org.rakam.analysis;

import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.rakam.database.AnalysisRuleDatabase;
import org.rakam.kume.Cluster;

import java.util.Map;
import java.util.Set;

/**
 * Created by buremba <Burak Emre Kabakcı> on 03/01/15 02:07.
 */
public class DefaultAnalysisRuleDatabase implements AnalysisRuleDatabase {
    public DefaultAnalysisRuleDatabase(Cluster cluster) {

    }

    @Override
    public Map<String, Set<AnalysisRule>> getAllRules() {
        return null;
    }

    @Override
    public void addRule(AnalysisRule rule) {

    }

    @Override
    public void deleteRule(AnalysisRule rule) {

    }
}
