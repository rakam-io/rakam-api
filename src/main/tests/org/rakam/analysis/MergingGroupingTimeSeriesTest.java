package org.rakam.analysis;

import org.junit.Before;
import org.junit.Test;
import org.rakam.RakamTestHelper;
import org.rakam.analysis.query.simple.SimpleFieldScript;
import org.rakam.analysis.rule.aggregation.TimeSeriesAggregationRule;
import org.rakam.cache.DistributedAnalysisRuleMap;
import org.rakam.cache.DistributedCacheAdapter;
import org.rakam.cache.local.LocalCacheAdapter;
import org.rakam.collection.EventAggregator;
import org.rakam.collection.PeriodicCollector;
import org.rakam.constant.AggregationAnalysis;
import org.rakam.database.DatabaseAdapter;
import org.rakam.util.ConversionUtil;
import org.rakam.util.SpanTime;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.rakam.constant.AggregationType.AVERAGE_X;
import static org.rakam.constant.AggregationType.COUNT;
import static org.rakam.constant.AggregationType.COUNT_X;
import static org.rakam.constant.AggregationType.MAXIMUM_X;
import static org.rakam.constant.AggregationType.MINIMUM_X;
import static org.rakam.constant.AggregationType.SUM_X;
import static org.rakam.constant.AggregationType.UNIQUE_X;
import static org.rakam.util.SpanTime.fromString;

/**
 * Created by buremba <Burak Emre Kabakcı> on 2/10/14 02:31.
 */
public class MergingGroupingTimeSeriesTest extends RakamTestHelper {
    LocalCacheAdapter localStorageAdapter = new LocalCacheAdapter();
    DistributedCacheAdapter cacheAdapter = new FakeDistributedCacheAdapter();
    DatabaseAdapter databaseAdapter = new DummyDatabase();
    EventAggregator eventAggregator = new EventAggregator(localStorageAdapter, cacheAdapter, databaseAdapter);
    PeriodicCollector collector = new PeriodicCollector(localStorageAdapter, cacheAdapter, databaseAdapter);
    EventAnalyzer eventAnalyzer = new EventAnalyzer(cacheAdapter, databaseAdapter);

    public static void assertMerged(JsonObject actual, JsonObject merged, int interval, BiFunction<Long, Long, Long> function) {
        JsonObject actualResult = actual.getObject("result");
        JsonObject mergedResult = merged.getObject("result");
        assertTrue("query result must contain at least one frame.", actualResult.size()>0);

        Map<String, List<String>> result = actualResult.getFieldNames().stream()
                .collect(Collectors.groupingBy(x -> new SpanTime(interval).span((int) (ConversionUtil.toLong(x) / 1000)).current() + "000"));
        result.forEach((key, items) -> {
            JsonObject grouped = new JsonObject();
            for (String item : items) {
                actualResult.getObject(item).toMap()
                        .forEach((actualKey, actualVal) -> {
                            if (!grouped.containsField(actualKey)) {
                                grouped.putNumber(actualKey, (Number) actualVal);
                            } else {
                                grouped.putNumber(actualKey, function.apply(grouped.getLong(actualKey), (long) actualVal));
                            }
                        });
            }

            final StringBuilder errMessage = new StringBuilder();
            errMessage.append(String.format("merged timestamp: %s keys:", key));
            items.forEach(x -> errMessage.append(" [" + x + ":" + actualResult.getObject(x) + "]"));
            errMessage.append("}");
            assertEquals(errMessage.toString(), grouped, mergedResult.getObject(key));
        });

    }

    @Test
    public void testCountAggregation() {
        String projectId = randomString(10);
        SpanTime interval = fromString("2day").spanCurrent();
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, COUNT, fromString("1day").period, null, null, new SimpleFieldScript<String>("key"));
        DistributedAnalysisRuleMap.add(projectId, rule);


        int start = interval.previous().current();
        int next = interval.next().current();
        for (int i = start; i < next; i++) {
            JsonObject m = new JsonObject().putString("key", "value" + i % 3);
            eventAggregator.aggregate(projectId, m, "actor" + i, i);
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject d0 = eventAnalyzer.analyze(AggregationAnalysis.COUNT, projectId, new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson())
                .putNumber("frame", 5));

        JsonObject d1 = eventAnalyzer.analyze(AggregationAnalysis.COUNT, projectId, new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson())
                .putNumber("interval", interval.period)
                .putNumber("frame", 5));

        assertMerged(d0, d1, interval.period, (x,y) -> x+y);
    }

    @Test
    public void testCountXAggregation() {
        String projectId = randomString(10);
        SpanTime interval = fromString("2day").spanCurrent();
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, COUNT_X, fromString("1day").period, new SimpleFieldScript<String>("key2"), null, new SimpleFieldScript<String>("key1"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        int start = interval.previous().current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().putString("key1", "value" + i % 2).putString("key2", "value"), "actor" + i, i);
            eventAggregator.aggregate(projectId, new JsonObject().putString("key1", "value" + i % 2), "actor" + i, i);
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject d0 = eventAnalyzer.analyze(AggregationAnalysis.COUNT_X, projectId,
                new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson())
                        .putNumber("frame", 5));

        JsonObject d1 = eventAnalyzer.analyze(AggregationAnalysis.COUNT_X, projectId,
                new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson())
                        .putNumber("interval", interval.period)
                        .putNumber("frame", 5));

        assertMerged(d0, d1, interval.period, (x,y) -> x+y);
    }


    @Test
    public void testSumXAggregation() {
        String projectId = randomString(10);
        SpanTime interval = fromString("2day").spanCurrent();
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, SUM_X, fromString("1day").period, new SimpleFieldScript<String>("key2"), null, new SimpleFieldScript<String>("key1"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        int start = interval.previous().current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            JsonObject m = new JsonObject().putString("key1", "value" + i % 3).putNumber("key2", 3);
            eventAggregator.aggregate(projectId, m, "actor" + i, i);
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject d0 = eventAnalyzer.analyze(AggregationAnalysis.SUM_X, projectId,
                new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson())
                        .putNumber("frame", 5));

        JsonObject d1 = eventAnalyzer.analyze(AggregationAnalysis.SUM_X, projectId,
                new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson())
                        .putNumber("interval", interval.period)
                        .putNumber("frame", 5));

        assertMerged(d0, d1, interval.period, (x,y) -> x+y);
    }


    @Test
    public void testMaxXAggregation() {
        String projectId = randomString(10);
        SpanTime interval = fromString("2day").spanCurrent();
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, MAXIMUM_X, fromString("1day").period, new SimpleFieldScript<String>("test"), null, new SimpleFieldScript<String>("key1"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        int start = interval.previous().current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().putNumber("test", i).putString("key1", "value"+i%3), "actor" + i, i);
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject d0 = eventAnalyzer.analyze(AggregationAnalysis.MAXIMUM_X, projectId,
                new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson())
                        .putNumber("frame", 5));

        JsonObject d1 = eventAnalyzer.analyze(AggregationAnalysis.MAXIMUM_X, projectId,
                new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson())
                        .putNumber("interval", interval.period)
                        .putNumber("frame", 5));

        assertMerged(d0, d1, interval.period, (x,y) -> Math.max(x, y));
    }


    @Test
    public void testMinXAggregation() {
        String projectId = randomString(10);
        SpanTime interval = fromString("2day").spanCurrent();
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, MINIMUM_X, fromString("1day").period, new SimpleFieldScript<String>("test"), null, new SimpleFieldScript<String>("key1"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        int start = interval.previous().current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            JsonObject m = new JsonObject().putNumber("test", i).putString("key1", "value" + i % 3);
            eventAggregator.aggregate(projectId, m, "actor" + i, i);
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject d0 = eventAnalyzer.analyze(AggregationAnalysis.MINIMUM_X, projectId,
                new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson())
                .putNumber("frame", 5));

        JsonObject d1 = eventAnalyzer.analyze(AggregationAnalysis.MINIMUM_X, projectId,
                new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson())
                    .putNumber("interval", interval.period)
                    .putNumber("frame", 5));

        assertMerged(d0, d1, interval.period, (x,y) -> Math.min(x, y));
    }


    @Test
    public void testAverageXAggregation() {
        String projectId = randomString(10);
        SpanTime interval = fromString("2day").spanCurrent();
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, AVERAGE_X, fromString("1day").period, new SimpleFieldScript<String>("test"), null, new SimpleFieldScript<String>("key1"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        int start = fromString("1day").spanCurrent().previous().previous().current();
        int next = fromString("1day").spanCurrent().current();

        for (int i = start; i < next; i++) {
            JsonObject m = new JsonObject().putNumber("test", i).putString("key1", "value" + i % 2);;
            eventAggregator.aggregate(projectId, m, "actor" + i, i);
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject d0 = eventAnalyzer.analyze(AggregationAnalysis.AVERAGE_X, projectId,
                new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson())
                        .putNumber("frame", 5));

        JsonObject d1 = eventAnalyzer.analyze(AggregationAnalysis.AVERAGE_X, projectId,
                new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson())
                        .putNumber("interval", interval.period)
                        .putNumber("frame", 5));

        // it may not work but in this specific test, the counts of each time interval are equal.
        assertMerged(d0, d1, interval.period, (x,y) -> (x+y)/2);
    }


    @Test
    public void testAverageXAggregation_whenSumX() {
        String projectId = randomString(10);
        SpanTime interval = fromString("2day").spanCurrent();
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, AVERAGE_X, fromString("1day").period, new SimpleFieldScript<String>("test"), null, new SimpleFieldScript<String>("key1"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        int start = interval.current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            JsonObject m = new JsonObject().putNumber("test", i).putString("key1", "value" + i % 3);
            eventAggregator.aggregate(projectId, m, "actor" + i, i);
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject d0 = eventAnalyzer.analyze(AggregationAnalysis.SUM_X, projectId,
                new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson())
                        .putNumber("frame", 5));

        JsonObject d1 = eventAnalyzer.analyze(AggregationAnalysis.SUM_X, projectId,
                new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson())
                        .putNumber("interval", interval.period)
                        .putNumber("frame", 5));

        assertMerged(d0, d1, interval.period, (x,y) -> x+y);
    }

    @Test
    public void testAverageXAggregation_whenCountX() {
        String projectId = randomString(10);
        SpanTime interval = fromString("2day").spanCurrent();
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, AVERAGE_X, fromString("1day").period, new SimpleFieldScript<String>("test"), null, new SimpleFieldScript<String>("key1"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        int start = interval.previous().current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            JsonObject m = new JsonObject().putNumber("test", i).putString("key1", "value" + i % 3);
            eventAggregator.aggregate(projectId, m, "actor" + i, i);
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject d0 = eventAnalyzer.analyze(AggregationAnalysis.COUNT_X, projectId,
                new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson())
                        .putNumber("frame", 5));

        JsonObject d1 = eventAnalyzer.analyze(AggregationAnalysis.COUNT_X, projectId,
                new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson())
                        .putNumber("interval", interval.period)
                        .putNumber("frame", 5));


        assertMerged(d0, d1, interval.period, (x, y) -> x + y);
    }

    @Test
    public void testUniqueXAggregation_countUniqueX() {
        String projectId = randomString(10);
        SpanTime interval = fromString("2day").spanCurrent();
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, UNIQUE_X, fromString("1day").period, new SimpleFieldScript<String>("test"), null, new SimpleFieldScript<String>("key1"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        int start = interval.previous().current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            JsonObject m = new JsonObject().putString("test", "value"+i).putString("key1", "value" + i % 3);
            eventAggregator.aggregate(projectId, m, "actor" + i, i);
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject d1 = eventAnalyzer.analyze(AggregationAnalysis.COUNT_UNIQUE_X, projectId,
                new JsonObject().putString("tracker", projectId)
                        .mergeIn(rule.toJson())
                        .putNumber("interval", interval.period)
                        .putNumber("frame", 5));

        final JsonObject result = d1.getObject("result");
        final String s = interval.previous().current() + "000";
        result.getFieldNames().forEach(key -> {
            final JsonObject items = result.getObject(key);
            if(s.equals(key)) {
                for (int i = 0; i < 3; i++) {
                    final long actual = (next - start) / 3L;
                    final double errorRate = Math.abs((actual - items.getNumber("value" + i).longValue()) / ((double) actual));
                    assertTrue("the error rate is higher than 2% : "+errorRate, errorRate <.02);
                }
            }else {
                assertEquals(items, new JsonObject());
            }
        });
    }

    @Test
    public void testUniqueXAggregation_selectUniqueX() {
        String projectId = randomString(10);
        SpanTime interval = fromString("2day").spanCurrent();
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, UNIQUE_X, fromString("1day").period, new SimpleFieldScript<String>("test"), null, new SimpleFieldScript<String>("key1"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        int start = interval.previous().current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            final String s = i > (start + ((next - start) / 2)) ? "1" : "0";
            JsonObject m = new JsonObject().putString("test", "value"+s+i % 3).putString("key1", "value" + i % 2);
            eventAggregator.aggregate(projectId, m, "actor" + i, i);
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject actualResult = eventAnalyzer.analyze(AggregationAnalysis.SELECT_UNIQUE_X, projectId,
                new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson())
                        .putNumber("frame", 5)).getObject("result");

        JsonObject mergedResult = eventAnalyzer.analyze(AggregationAnalysis.SELECT_UNIQUE_X, projectId,
                new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson())
                        .putNumber("interval", interval.period)
                        .putNumber("frame", 5)).getObject("result");

        Map<String, List<String>> result = actualResult.getFieldNames().stream()
                .collect(Collectors.groupingBy(x -> new SpanTime(interval.period).span((int) (ConversionUtil.toLong(x) / 1000)).current() + "000"));
        result.forEach((key, items) -> {
            JsonObject grouped = new JsonObject();
            for (String item : items) {
                actualResult.getObject(item).toMap()
                        .forEach((actualKey, actualVal) -> {
                            if (!grouped.containsField(actualKey)) {
                                grouped.putArray(actualKey, new JsonArray((List<Object>) actualVal));
                            } else {
                                ((List) actualVal).forEach(grouped.getArray(actualKey)::add);
                            }
                        });
            }

            assertEquals(grouped, mergedResult.getObject(key));
        });
    }

    @Before
    public void clear() {
        localStorageAdapter.flush();
        cacheAdapter.flush();
        databaseAdapter.flush();
        DistributedAnalysisRuleMap.clear();
    }
}