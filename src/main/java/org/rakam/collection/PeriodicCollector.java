package org.rakam.collection;

import org.rakam.ServiceStarter;
import org.rakam.analysis.rule.AnalysisRuleList;
import org.rakam.analysis.rule.aggregation.AggregationRule;
import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.rakam.analysis.rule.aggregation.MetricAggregationRule;
import org.rakam.analysis.rule.aggregation.TimeSeriesAggregationRule;
import org.rakam.cache.CacheAdapter;
import org.rakam.constant.Analysis;
import org.rakam.database.DatabaseAdapter;
import org.rakam.database.KeyValueStorage;
import org.rakam.util.SpanTime;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by buremba on 14/05/14.
 */
public class PeriodicCollector {
    final static CacheAdapter fastCacheAdapter = CollectionWorker.localCacheAdapter;
    final static CacheAdapter cacheAdapter = ServiceStarter.injector.getInstance(CacheAdapter.class);
    final static DatabaseAdapter databaseAdapter = ServiceStarter.injector.getInstance(DatabaseAdapter.class);

    public static void process(Map.Entry entry) {
        AnalysisRuleList value = (AnalysisRuleList) entry.getValue();

        for (AnalysisRule rule : value) {
            AggregationRule mrule;
            String previous_key;
            String now_key;
            if (rule.analysisType() == Analysis.ANALYSIS_TIMESERIES) {
                mrule = (AggregationRule) rule;
                SpanTime time = new SpanTime(((TimeSeriesAggregationRule) rule).interval.period).span((int) (System.currentTimeMillis() / 1000));
                int now = time.current();
                int previous = time.getPrevious().current();
                previous_key = mrule.id + ":" + previous;
                now_key = mrule.id + ":" + now;
            } else if (rule.analysisType() == Analysis.ANALYSIS_METRIC) {
                mrule = (MetricAggregationRule) rule;
                previous_key = mrule.id;
                now_key = mrule.id;
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
                        moveCounters(previous_key, cacheAdapter, databaseAdapter);
                        break;
                    case SELECT_UNIQUE_X:
                    case COUNT_UNIQUE_X:
                        moveSets(now_key, fastCacheAdapter, cacheAdapter);
                        moveSets(previous_key, cacheAdapter, databaseAdapter);
                }
            else
                switch (mrule.type) {
                    case COUNT:
                    case COUNT_X:
                    case MAXIMUM_X:
                    case MINIMUM_X:
                    case SUM_X:
                        moveGroupByCounter(now_key, fastCacheAdapter, cacheAdapter);
                        moveGroupByCounter(previous_key, cacheAdapter, databaseAdapter);
                        break;
                    case SELECT_UNIQUE_X:
                    case COUNT_UNIQUE_X:
                        moveGroupBySet(now_key, fastCacheAdapter, cacheAdapter);
                        moveGroupBySet(previous_key, cacheAdapter, databaseAdapter);
                }
        }

        fastCacheAdapter.flush();
        cacheAdapter.setCounter(ServiceStarter.server_id, System.currentTimeMillis() / 1000);
    }


    public static void moveGroupByCounter(String key, KeyValueStorage from, KeyValueStorage to) {
        Set<String> keys = from.getSet(key + "::keys");
        if(keys!=null) {
            to.addSet(key + "::keys", keys);
            for (String k : keys) {
                moveCounters(key + ":" + k, from, to);
            }
        }
    }
    public static void moveGroupBySet(String key, KeyValueStorage from, KeyValueStorage to) {
        Set<String> keys = from.getSet(key + "::keys");
        if(keys!=null) {
            to.addSet(key + "::keys", keys);
            for (String k : keys) {
                to.addSet(key + ":" + k, from.getSet(key+":"+k));
            }
        }
    }

    public static void moveCounters(String key, KeyValueStorage from, KeyValueStorage to) {
        to.incrementCounter(key, from.getCounter(key));
    }

    public static void moveSets(String key, KeyValueStorage from, KeyValueStorage to) {
        Iterator<String> keys = from.getSetIterator(key + "::keys");
        if (keys != null) {
            HashSet<String> s = new HashSet();
            while (keys.hasNext()) {
                String item = keys.next();
                s.add(item);
                Iterator<String> it = from.getSetIterator(key + ":" + item);
                if (it != null) {
                    HashSet<String> set = new HashSet();
                    while (it.hasNext())
                        set.add(it.next());
                    to.addSet(key + ":" + item, set);
                }
            }
            to.addSet(key + "::keys", s);

        }
    }
}
