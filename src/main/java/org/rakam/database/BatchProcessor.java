package org.rakam.database;

import org.rakam.analysis.rule.AnalysisRule;
import org.rakam.cache.SimpleCacheAdapter;

/**
 * Created by buremba on 12/05/14.
 */
public interface BatchProcessor {
    public void processRule(AnalysisRule rule);
    public void exportCurrentToCache(SimpleCacheAdapter cacheAdapter, AnalysisRule rule);
}