package org.rakam.collection;

import org.rakam.ServiceStarter;
import org.rakam.analysis.rule.AnalysisRuleList;
import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.rakam.analysis.rule.aggregation.TimeSeriesAggregationRule;
import org.rakam.cache.CacheAdapter;
import org.rakam.constant.AggregationType;
import org.rakam.constant.Analysis;
import org.rakam.database.DatabaseAdapter;
import org.rakam.util.SpanTime;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by buremba on 14/05/14.
 */
public class PeriodicCollector {
    final static CacheAdapter cacheAdapter = CollectionWorker.localCacheAdapter;
    final static DatabaseAdapter databaseAdapter = ServiceStarter.injector.getInstance(DatabaseAdapter.class);

    public static void process(Map.Entry entry) {
        AnalysisRuleList value = (AnalysisRuleList) entry.getValue();

        for (AnalysisRule rule : value) {
            if (rule.analysisType() == Analysis.ANALYSIS_TIMESERIES) {
                TimeSeriesAggregationRule mrule = (TimeSeriesAggregationRule) rule;
                int now = new SpanTime(mrule.interval.period).span((int) (System.currentTimeMillis() / 1000)).current();
                String key = mrule.id + ":" + now;
                switch (mrule.type) {
                    case COUNT:
                    case COUNT_X:
                    case MAXIMUM_X:
                    case MINIMUM_X:
                    case SUM_X:
                        databaseAdapter.incrementCounter(key, cacheAdapter.getCounter(key));
                        break;
                    case AVERAGE_X:
                        String sum_id = mrule.buildId(rule.project, AggregationType.SUM_X, mrule.select, mrule.filters, mrule.groupBy, mrule.interval);
                        String count_id = mrule.buildId(rule.project, AggregationType.COUNT_X, mrule.select, mrule.filters, mrule.groupBy, mrule.interval);
                        databaseAdapter.incrementCounter(sum_id + ":" + mrule.interval.current(), cacheAdapter.getCounter(sum_id));
                        databaseAdapter.incrementCounter(count_id + ":" + mrule.interval.current(), cacheAdapter.getCounter(count_id));
                        break;
                    case SELECT_UNIQUE_X:
                    case COUNT_UNIQUE_X:
                        Iterator<String> keys = cacheAdapter.getSetIterator(key + "::keys");
                        if(keys!=null) {
                            HashSet<String> s = new HashSet();
                            while (keys.hasNext()) {
                                String item = keys.next();
                                s.add(item);
                                Iterator<String> it = cacheAdapter.getSetIterator(key + ":" + item);
                                if(it!=null) {
                                    HashSet<String> set = new HashSet();
                                    while (it.hasNext())
                                        set.add(it.next());
                                    databaseAdapter.addSet(key + ":" + item, set);
                                }
                            }
                            databaseAdapter.addSet(key + "::keys", s);

                        }
                }
            }
            cacheAdapter.flush();
        }
    }
}
