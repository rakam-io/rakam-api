package org.rakam.database;

import org.rakam.analysis.rule.aggregation.AnalysisRule;

/**
 * Created by buremba on 12/05/14.
 */
public interface BatchProcessor {
    public void processRule(AnalysisRule rule);
    public void processRule(AnalysisRule rule, long start_time, long end_time);
    public void exportCurrentToCache(org.rakam.cache.CacheAdapter cacheAdapter, AnalysisRule rule);
}