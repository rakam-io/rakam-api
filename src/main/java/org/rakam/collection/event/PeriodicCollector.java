package org.rakam.collection.event;

import com.google.inject.Injector;
import org.rakam.analysis.rule.aggregation.AggregationRule;
import org.rakam.analysis.rule.aggregation.AnalysisRule;
import org.rakam.analysis.rule.aggregation.TimeSeriesAggregationRule;
import org.rakam.stream.AverageCounter;
import org.rakam.stream.MetricStreamAdapter;
import org.rakam.stream.TimeSeriesStreamAdapter;
import org.rakam.stream.local.LocalCache;

import java.util.Map;
import java.util.Set;

import static org.rakam.constant.Analysis.ANALYSIS_METRIC;
import static org.rakam.constant.Analysis.ANALYSIS_TIMESERIES;

/**
 * Created by buremba on 14/05/14.
 */
public class PeriodicCollector {
    private final LocalCache localCacheAdapter;
    private final MetricStreamAdapter metricStorage;
    private final TimeSeriesStreamAdapter timeSeriesStorage;

    public PeriodicCollector(Injector injector) {
        this.localCacheAdapter = injector.getInstance(LocalCache.class);
        this.metricStorage = injector.getInstance(MetricStreamAdapter.class);
        this.timeSeriesStorage = injector.getInstance(TimeSeriesStreamAdapter.class);
    }

    public void process(Set<Map.Entry<String, Set<AnalysisRule>>> entries) {
        for (Map.Entry<String, Set<AnalysisRule>> entry : entries) {
            Set<AnalysisRule> value = entry.getValue();

            for (AnalysisRule analysisRule : value) {
                AggregationRule rule = (AggregationRule) analysisRule;

                String id = rule.id();
                if (rule.analysisType().equals(ANALYSIS_METRIC)) {
                    if (rule.groupBy == null)
                        switch (rule.type) {
                            case COUNT:
                            case COUNT_X:
                            case SUM_X: {
                                Long metricCounter = localCacheAdapter.getCounter(id);
                                if (metricCounter != null && metricCounter > 0)
                                    metricStorage.incrementMetricCounter(id, metricCounter);
                                break;
                            }
                            case MAXIMUM_X:
                            case MINIMUM_X: {
                                Long metricCounter = localCacheAdapter.getCounter(id);
                                if (metricCounter != null)
                                    metricStorage.setMetricCounter(id, metricCounter);
                                break;
                            }
                            case AVERAGE_X: {
                                AverageCounter metricCounter = localCacheAdapter.getAverageCounter(id);
                                if (metricCounter != null && metricCounter.getCount() > 0)
                                    metricStorage.incrementMetricAverageCounter(id, metricCounter.getSum(), metricCounter.getCount());
                                break;
                            }
                            case UNIQUE_X:
                                Set<String> metricSet = localCacheAdapter.getSet(id);
                                if (metricSet != null)
                                    metricStorage.addMetricSet(id, metricSet);
                                break;
                        }
                    else
                        switch (rule.type) {
                            case COUNT:
                            case COUNT_X:
                            case MAXIMUM_X:
                            case MINIMUM_X:
                            case SUM_X: {
                                Map<String, Long> metricGroupByCounters = localCacheAdapter.getGroupByCounters(id);
                                if (metricGroupByCounters != null)
                                    metricStorage.incrementMetricGroupByCounters(id, metricGroupByCounters);
                                break;
                            }
                            case AVERAGE_X: {
                                Map<String, AverageCounter> metricGroupByCounters = localCacheAdapter.getGroupByAverageCounters(id);
                                if (metricGroupByCounters != null)
                                    metricStorage.incrementMetricAverageCounters(id, metricGroupByCounters);
                                break;
                            }
                            case UNIQUE_X:
                                Map<String, Set<String>> metricGroupByStrings = localCacheAdapter.getGroupByStrings(id);
                                if (metricGroupByStrings != null)
                                    metricStorage.addMetricGroupByStrings(id, metricGroupByStrings);
                                break;
                        }
                } else if (rule.analysisType().equals(ANALYSIS_TIMESERIES)) {
                    Set<String> frames = localCacheAdapter.getSet("frame:" + rule.id());
                    if (frames != null)
                        for (String previousFrame : frames) {
                            flushTimeSeries((TimeSeriesAggregationRule) rule, Integer.parseInt(previousFrame));
                        }
                }

            }
        }

        localCacheAdapter.flush();
//        cacheAdapter.setCounter(Long.toString(MemberShipListener.getServerId()), UTCTime());
    }

    public void flushTimeSeries(TimeSeriesAggregationRule rule, Integer time) {
        String id = rule.id();

        if (rule.groupBy == null)
            switch (rule.type) {
                case COUNT:
                case COUNT_X:
                case SUM_X: {
                    Long counter = localCacheAdapter.getCounter(id + ":" + time);
                    if (counter != null && counter > 0)
                        timeSeriesStorage.incrementTimeSeriesCounter(id, time, counter);
                    break;
                }
                case MAXIMUM_X:
                case MINIMUM_X: {
                    Long counter = localCacheAdapter.getCounter(id + ":" + time);
                    if (counter != null)
                        timeSeriesStorage.setTimeSeriesCounter(id, time, counter);
                    break;
                }
                case AVERAGE_X: {
                    AverageCounter counter = localCacheAdapter.getAverageCounter(id + ":" + time);
                    if (counter != null && counter.getCount() > 0)
                        timeSeriesStorage.incrementTimeSeriesAverageCounter(id, time, counter.getSum(), counter.getCount());
                    break;
                }
                case UNIQUE_X: {
                    Set<String> sets = localCacheAdapter.getSet(id + ":" + time);
                    if (sets != null && sets.size() > 0)
                        timeSeriesStorage.addTimeSeriesStrings(id, time, sets);
                    break;
                }
            }
        else
            switch (rule.type) {
                case COUNT:
                case COUNT_X:
                case MAXIMUM_X:
                case MINIMUM_X:
                case SUM_X:
                    Map<String, Long> counters = localCacheAdapter.getGroupByCounters(id + ":" + time);
                    if (counters != null && counters.size() > 0)
                        timeSeriesStorage.incrementTimeSeriesGroupByCounters(id, time, counters);
                    break;
                case AVERAGE_X:
                    Map<String, AverageCounter> counter = localCacheAdapter.getGroupByAverageCounters(id + ":" + time);
                    if (counter != null && counter.size() > 0)
                        timeSeriesStorage.incrementTimeSeriesGroupByAverageCounters(id, time, counter);
                    break;
                case UNIQUE_X:
                    Map<String, Set<String>> sets = localCacheAdapter.getGroupByStrings(id + ":" + time);
                    if (sets != null && sets.size() > 0)
                        timeSeriesStorage.addTimeSeriesGroupByStrings(id, time, sets);
                    break;
            }
    }
}
