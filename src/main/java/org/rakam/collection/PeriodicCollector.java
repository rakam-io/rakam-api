package org.rakam.collection;

import org.rakam.ServiceStarter;
import org.rakam.analysis.rule.aggregation.AggregationRule;
import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.rakam.analysis.rule.aggregation.MetricAggregationRule;
import org.rakam.analysis.rule.aggregation.TimeSeriesAggregationRule;
import org.rakam.cache.CacheAdapter;
import org.rakam.cache.hazelcast.models.AverageCounter;
import org.rakam.constant.Analysis;
import org.rakam.database.DatabaseAdapter;
import org.rakam.database.KeyValueStorage;
import org.rakam.util.SpanTime;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by buremba on 14/05/14.
 */
public class    PeriodicCollector {
    final static CacheAdapter fastCacheAdapter = CollectionWorker.localStorageAdapter;
    final static CacheAdapter cacheAdapter = ServiceStarter.injector.getInstance(CacheAdapter.class);
    final static DatabaseAdapter databaseAdapter = ServiceStarter.injector.getInstance(DatabaseAdapter.class);

    public static void process(Map.Entry entry) {
        HashSet<AnalysisRule> value = (HashSet<AnalysisRule>) entry.getValue();

        for (AnalysisRule rule : value) {
            AggregationRule mrule;
            Analysis analysisType = rule.analysisType();
            String previous_key;
            String now_key;
            if (analysisType == Analysis.ANALYSIS_TIMESERIES) {
                mrule = (AggregationRule) rule;
                SpanTime time = new SpanTime(((TimeSeriesAggregationRule) rule).interval.period).span((int) (System.currentTimeMillis() / 1000));
                int now = time.current();
                int previous = time.getPrevious().current();
                previous_key = mrule.getId() + ":" + previous;
                now_key = mrule.id() + ":" + now;
            } else if (rule.analysisType() == Analysis.ANALYSIS_METRIC) {
                mrule = (MetricAggregationRule) rule;
                now_key = mrule.id();
                previous_key = null;
            } else {
                throw new IllegalStateException();
            }
            if (mrule.groupBy == null)
                switch (mrule.type) {
                    case COUNT:
                    case COUNT_X:
                    case MAXIMUM_X:
                    case MINIMUM_X:
                    case SUM_X:
                        moveCounters(now_key, fastCacheAdapter, cacheAdapter);
                        if (analysisType == Analysis.ANALYSIS_TIMESERIES)
                            moveCounters(previous_key, cacheAdapter, databaseAdapter);
                        break;
                    case AVERAGE_X:
                        // TODO
                        moveAverageCounters(now_key, fastCacheAdapter, cacheAdapter);
                        if (analysisType == Analysis.ANALYSIS_TIMESERIES)
                            moveAverageCounters(previous_key, cacheAdapter, databaseAdapter);
                        break;
                    case UNIQUE_X:
                        moveSet(now_key, fastCacheAdapter, cacheAdapter);
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
                        moveGroupByOrderedCounter(now_key, fastCacheAdapter, cacheAdapter);
                        if (analysisType == Analysis.ANALYSIS_TIMESERIES)
                            moveGroupByOrderedCounter(previous_key, cacheAdapter, databaseAdapter);
                        break;
                    case AVERAGE_X:
                        moveGroupByOrderedAverageCounter(now_key, fastCacheAdapter, cacheAdapter);
                        if (analysisType == Analysis.ANALYSIS_TIMESERIES)
                            moveGroupByOrderedCounter(previous_key, cacheAdapter, databaseAdapter);
                    case UNIQUE_X:
                        moveGroupBySet(now_key, fastCacheAdapter, cacheAdapter);
                        if (analysisType == Analysis.ANALYSIS_TIMESERIES)
                            moveGroupBySet(previous_key, cacheAdapter, databaseAdapter);
                }

        }

        fastCacheAdapter.flush();
        cacheAdapter.setCounter(Long.toString(ServiceStarter.server_id), System.currentTimeMillis() / 1000);
    }

    private static void moveAverageCounters(String now_key, CacheAdapter fastCacheAdapter, CacheAdapter cacheAdapter) {
        AverageCounter averageCounter = fastCacheAdapter.getAverageCounter(now_key);
        cacheAdapter.incrementAverageCounter(now_key, averageCounter.getSum(), averageCounter.getCount());
    }

    private static void moveGroupByOrderedAverageCounter(String key, CacheAdapter from, CacheAdapter to) {
        Set<String> keys = from.getSet(key);
        if (keys != null) {
            for (String k : keys) {
                Long counter = from.getCounter(key + ":count:" + k);
                Long sum = from.getCounter(key + ":sum:" + k);
                if (counter != null) {
                    to.incrementGroupByAverageCounter(key, k, sum, counter);
                    from.removeCounter(key + ":" + k);
                }
            }
        }
    }

    private static void moveGroupByOrderedCounter(String key, CacheAdapter from, CacheAdapter to) {
        Map<String, Long> groupByCounters = from.getGroupByCounters(key);
        if (groupByCounters != null)
            for (Map.Entry<String, Long> k : groupByCounters.entrySet()) {
                to.addGroupByCounter(key, k.getKey(), k.getValue());
            }
        from.removeGroupByCounters(key);
    }

    public static void moveGroupBySet(String key, CacheAdapter from, CacheAdapter to) {
        Map<String, Set<String>> groupByStrings = from.getGroupByStrings(key);
        if(groupByStrings!=null)
        for(Map.Entry<String, Set<String>> item : groupByStrings.entrySet()) {
            to.addGroupByString(key, item.getKey(), item.getValue());
        }
        from.removeGroupByStrings(key);
    }

    public static void moveCounters(String key, KeyValueStorage from, KeyValueStorage to) {
        Long co = from.getCounter(key);
        if(co!=null) {
            to.incrementCounter(key, from.getCounter(key));
            from.removeCounter(key);
        }
    }

    public static void moveSet(String key, KeyValueStorage from, KeyValueStorage to) {
        to.addSet(key, from.getSet(key));
        from.removeSet(key);
    }
}
