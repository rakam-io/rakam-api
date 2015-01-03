package org.rakam.database;

import org.rakam.analysis.rule.aggregation.AnalysisRule;

import java.util.Map;
import java.util.Set;

/**
 * Created by buremba <Burak Emre Kabakcı> on 21/07/14 05:21.
 */
public interface AnalysisRuleDatabase {
    Map<String, Set<AnalysisRule>> getAllRules();

    void add(AnalysisRule rule);

    void delete(AnalysisRule rule);

    Set<AnalysisRule> get(String project);

    void clear();
}
