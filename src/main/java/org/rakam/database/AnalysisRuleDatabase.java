package org.rakam.database;

import org.rakam.analysis.rule.aggregation.AnalysisRule;

import java.util.HashSet;
import java.util.Map;

/**
 * Created by buremba <Burak Emre Kabakcı> on 21/07/14 05:21.
 */
public interface AnalysisRuleDatabase {
    Map<String, HashSet<AnalysisRule>> getAllRules();
    void addRule(AnalysisRule rule);
    void deleteRule(AnalysisRule rule);
}
