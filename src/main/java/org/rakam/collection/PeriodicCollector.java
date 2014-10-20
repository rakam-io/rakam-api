package org.rakam.collection;

import org.rakam.analysis.AverageCounter;
import org.rakam.analysis.rule.aggregation.AggregationRule;
import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.rakam.analysis.rule.aggregation.MetricAggregationRule;
import org.rakam.analysis.rule.aggregation.TimeSeriesAggregationRule;
import org.rakam.cache.CacheAdapter;
import org.rakam.cache.DistributedCacheAdapter;
import org.rakam.cache.local.LocalCacheAdapter;
import org.rakam.cluster.MemberShipListener;
import org.rakam.constant.Analysis;
import org.rakam.database.DatabaseAdapter;
import org.rakam.database.KeyValueStorage;
import org.rakam.util.Interval;

import java.util.Map;
import java.util.Set;

import static org.rakam.util.DateUtil.UTCTime;

/**
 * Created by buremba on 14/05/14.
 */
public class PeriodicCollector {
    private final LocalCacheAdapter localCacheAdapter;
    private final DatabaseAdapter databaseAdapter;
    private final DistributedCacheAdapter cacheAdapter;

    public PeriodicCollector(LocalCacheAdapter fastCacheAdapter, DistributedCacheAdapter cacheAdapter, DatabaseAdapter databaseAdapter) {
        this.localCacheAdapter = fastCacheAdapter;
        this.cacheAdapter = cacheAdapter;
        this.databaseAdapter = databaseAdapter;
    }

    public void process(Map.Entry<String, Set<AnalysisRule>> entry) {
        Set<AnalysisRule> value = entry.getValue();

        for (AnalysisRule rule : value) {
            AggregationRule mrule;
            Analysis analysisType = rule.analysisType();
            String previous_key;
            String now_key;
            switch (analysisType) {
                case ANALYSIS_TIMESERIES:
                    mrule = (AggregationRule) rule;
                    Interval.StatefulSpanTime time = ((TimeSeriesAggregationRule) mrule).interval.spanCurrent();

                    now_key = rule.id() + ":" + time.current();
                    previous_key = rule.id() + ":" + time.previous().current();
                    break;
                case ANALYSIS_METRIC:
                    mrule = (MetricAggregationRule) rule;
                    now_key = rule.id();
                    previous_key = null;
                    break;
                default:
                    throw new IllegalStateException();

            }
            if (mrule.groupBy == null)
                switch (mrule.type) {
                    case COUNT:
                    case COUNT_X:
                    case MAXIMUM_X:
                    case MINIMUM_X:
                    case SUM_X:
                        moveCounters(now_key, localCacheAdapter, cacheAdapter);
                        if (analysisType == Analysis.ANALYSIS_TIMESERIES)
                            moveCounters(previous_key, cacheAdapter, databaseAdapter);
                        break;
                    case AVERAGE_X:
                        moveAverageCounters(now_key, localCacheAdapter, cacheAdapter);
                        if (analysisType == Analysis.ANALYSIS_TIMESERIES)
                            moveAverageCounters(previous_key, cacheAdapter, databaseAdapter);
                        break;
                    case UNIQUE_X:
                        moveSet(now_key, localCacheAdapter, cacheAdapter);
                        if (analysisType == Analysis.ANALYSIS_TIMESERIES)
                            moveSet(previous_key, cacheAdapter, databaseAdapter);
                        break;

                }
            else
                switch (mrule.type) {
                    case COUNT:
                    case COUNT_X:
                    case MAXIMUM_X:
                    case MINIMUM_X:
                    case SUM_X:
                        moveGroupByOrderedCounter(now_key, localCacheAdapter, cacheAdapter);
                        if (analysisType == Analysis.ANALYSIS_TIMESERIES)
                            moveGroupByOrderedCounter(previous_key, cacheAdapter, databaseAdapter);
                        break;
                    case AVERAGE_X:
                        moveGroupByOrderedAverageCounter(now_key, localCacheAdapter, cacheAdapter);
                        if (analysisType == Analysis.ANALYSIS_TIMESERIES)
                            moveGroupByOrderedCounter(previous_key, cacheAdapter, databaseAdapter);
                    case UNIQUE_X:
                        moveGroupBySet(now_key, localCacheAdapter, cacheAdapter);
                        if (analysisType == Analysis.ANALYSIS_TIMESERIES)
                            moveGroupBySet(previous_key, cacheAdapter, databaseAdapter);
                }

        }

        localCacheAdapter.flush();
        cacheAdapter.setCounter(Long.toString(MemberShipListener.getServerId()), UTCTime());
    }

    private void moveAverageCounters(String now_key, CacheAdapter fastCacheAdapter, CacheAdapter cacheAdapter) {
        AverageCounter averageCounter = fastCacheAdapter.getAverageCounter(now_key);
        if(averageCounter!=null)
        cacheAdapter.incrementAverageCounter(now_key, averageCounter.getSum(), averageCounter.getCount());
    }

    private void moveGroupByOrderedAverageCounter(String key, CacheAdapter from, CacheAdapter to) {
        Map<String, AverageCounter> groupByCounters = from.getGroupByAverageCounters(key);
        if (groupByCounters != null)
            for (Map.Entry<String, AverageCounter> k : groupByCounters.entrySet()) {
                AverageCounter value = k.getValue();
                to.incrementGroupByAverageCounter(key, k.getKey(), value.getSum(), value.getCount());
            }
        from.removeGroupByCounters(key);
    }

    private void moveGroupByOrderedCounter(String key, CacheAdapter from, CacheAdapter to) {
        Map<String, Long> groupByCounters = from.getGroupByCounters(key);
        if (groupByCounters != null)
            for (Map.Entry<String, Long> k : groupByCounters.entrySet()) {
                to.incrementGroupBySimpleCounter(key, k.getKey(), k.getValue());
            }
        from.removeGroupByCounters(key);
    }

    private void moveGroupBySet(String key, CacheAdapter from, CacheAdapter to) {
        Map<String, Set<String>> groupByStrings = from.getGroupByStrings(key);
        if(groupByStrings!=null)
        for(Map.Entry<String, Set<String>> item : groupByStrings.entrySet()) {
            to.addGroupByString(key, item.getKey(), item.getValue());
        }
        from.removeGroupByStrings(key);
    }

    private void moveCounters(String key, KeyValueStorage from, KeyValueStorage to) {
        Long co = from.getCounter(key);
        if(co!=null && co>0) {
            to.incrementCounter(key, co);
            from.removeCounter(key);
        }
    }

    private void moveSet(String key, KeyValueStorage from, KeyValueStorage to) {
        to.addSet(key, from.getSet(key));
        from.removeSet(key);
    }
}
