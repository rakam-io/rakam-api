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
import org.rakam.util.SpanTime;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
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
 * Created by buremba <Burak Emre Kabakcı> on 28/09/14 20:48.
 */
public class GroupingTimeSeriesAnalysisTest extends RakamTestHelper {
    LocalCacheAdapter localStorageAdapter = new LocalCacheAdapter();
    DistributedCacheAdapter cacheAdapter = new FakeDistributedCacheAdapter();
    DatabaseAdapter databaseAdapter = new DummyDatabase();
    EventAggregator eventAggregator = new EventAggregator(localStorageAdapter, cacheAdapter, databaseAdapter);
    PeriodicCollector collector = new PeriodicCollector(localStorageAdapter, cacheAdapter, databaseAdapter);
    EventAnalyzer eventAnalyzer = new EventAnalyzer(cacheAdapter, databaseAdapter);

    @Test
    public void testCountAggregation() {
        String projectId = randomString(10);
        SpanTime interval = fromString("1day").spanCurrent();
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, COUNT,  interval.period, null, null, new SimpleFieldScript<String>("key"));
        DistributedAnalysisRuleMap.add(projectId, rule);


        int start = interval.current();
        int next = interval.next().current();
        for (int i = start; i < next; i++) {
            JsonObject m = new JsonObject().putString("key", "value" + i % 3);
            eventAggregator.aggregate(projectId, m, "actor" + i, i);
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson()).putNumber("frame", 5);
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.COUNT, projectId, query);

        JsonObject jsonObject = new JsonObject().putNumber("value0", 28800L).putNumber("value1", 28800L).putNumber("value2", 28800L);
        JsonObject json = new JsonObject().putObject(interval.previous().current() + "000", jsonObject);
        for (int i = 0; i < 4; i++)
            json.putObject(interval.previous().current()+"000", new JsonObject());

        assertEquals(json, data.getObject("result"));
    }

    @Test
    public void testCountXAggregation() {
        String projectId = randomString(10);
        SpanTime interval = fromString("1day").spanCurrent();
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, COUNT_X, interval.period, new SimpleFieldScript<String>("key2"), null, new SimpleFieldScript<String>("key1"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        int start = interval.current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().putString("key1", "value" + i % 2).putString("key2", "value"), "actor" + i, i);
            eventAggregator.aggregate(projectId, new JsonObject().putString("key1", "value" + i % 2), "actor" + i, i);
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson()).putNumber("frame", 5);
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.COUNT_X, projectId, query);

        long value = (next - start)/2;
        JsonObject jsonObject = new JsonObject().putNumber("value0", value).putNumber("value1", value);
        JsonObject json = new JsonObject().putObject(interval.previous().current() + "000", jsonObject);
        for (int i = 0; i < 4; i++)
            json.putObject(interval.previous().current() + "000", new JsonObject());

        assertEquals(json, data.getObject("result"));
    }


    @Test
    public void testSumXAggregation() {
        String projectId = randomString(10);
        SpanTime interval = fromString("1day").spanCurrent();
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, SUM_X, interval.period, new SimpleFieldScript<String>("key2"), null, new SimpleFieldScript<String>("key1"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        int start = interval.current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            JsonObject m = new JsonObject().putString("key1", "value" + i % 3).putNumber("key2", 3);
            eventAggregator.aggregate(projectId, m, "actor" + i, i);
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson()).putNumber("frame", 5);
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.SUM_X, projectId, query);

        long value = (next - start);
        JsonObject jsonObject = new JsonObject().putNumber("value0", value).putNumber("value1", value).putNumber("value2", value);
        JsonObject json = new JsonObject().putObject(interval.previous().current() + "000", jsonObject);
        for (int i = 0; i < 4; i++)
            json.putObject(interval.previous().current() + "000", new JsonObject());

        assertEquals(json, data.getObject("result"));
    }


    @Test
    public void testMaxXAggregation() {
        String projectId = randomString(10);
        SpanTime interval = fromString("1day").spanCurrent();
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, MAXIMUM_X, interval.period, new SimpleFieldScript<String>("test"),null, new SimpleFieldScript<String>("key1"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        int start = interval.current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().putNumber("test", i).putString("key1", "value"+i%3), "actor" + i, i);
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson()).putNumber("frame", 5);
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.MAXIMUM_X, projectId, query);

        long value = (next - start)/3;
        JsonObject jsonObject = new JsonObject().putNumber("value0", value).putNumber("value1", value).putNumber("value2", value);
        JsonObject json = new JsonObject().putObject(interval.previous().current() + "000", jsonObject);
        for (int i = 0; i < 4; i++)
            json.putObject(interval.previous().current() + "000", new JsonObject());

        assertEquals(json, data.getObject("result"));
    }


    @Test
    public void testMinXAggregation() {
        String projectId = randomString(10);
        SpanTime interval = fromString("1day").spanCurrent();
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, MINIMUM_X, interval.period, new SimpleFieldScript<String>("test"), null, new SimpleFieldScript<String>("key1"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        int start = interval.current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            JsonObject m = new JsonObject().putNumber("test", i).putString("key1", "value" + i % 3);
            eventAggregator.aggregate(projectId, m, "actor" + i, i);
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson()).putNumber("frame", 5);        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.MINIMUM_X, projectId, query);

        JsonObject jsonObject = new JsonObject().putNumber("value0", (long) start).putNumber("value1", start+1L).putNumber("value2", start + 2L);
        JsonObject json = new JsonObject().putObject(interval.previous().current() + "000", jsonObject);
        for (int i = 0; i < 4; i++)
            json.putObject(interval.previous().current() + "000", new JsonObject());

        assertEquals(json, data.getObject("result"));
    }


    @Test
    public void testAverageXAggregation() {
        String projectId = randomString(10);
        SpanTime interval = fromString("1day").spanCurrent();
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, AVERAGE_X, interval.period, new SimpleFieldScript<String>("test"), null, new SimpleFieldScript<String>("key1"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        int start = interval.current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            JsonObject m = new JsonObject().putNumber("test", i).putString("key1", "value" + i % 3);;
            eventAggregator.aggregate(projectId, m, "actor" + i, i);
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson()).putNumber("frame", 5);
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.AVERAGE_X, projectId, query);

        JsonObject jsonObject = new JsonObject().putNumber("value0", 1412078398L).putNumber("value1", 1412078399L).putNumber("value2", 1412078400L);
        JsonObject json = new JsonObject().putObject(interval.previous().current() + "000", jsonObject);
        for (int i = 0; i < 4; i++)
            json.putObject(interval.previous().current() + "000", new JsonObject());

        assertEquals(json, data.getObject("result"));
    }


    @Test
    public void testAverageXAggregation_whenSumX() {
        String projectId = randomString(10);
        SpanTime interval = fromString("1day").spanCurrent();
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, AVERAGE_X, interval.period, new SimpleFieldScript<String>("test"), null, new SimpleFieldScript<String>("key1"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        int start = interval.current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            JsonObject m = new JsonObject().putNumber("test", i).putString("key1", "value" + i % 3);
            eventAggregator.aggregate(projectId, m, "actor" + i, i);
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson()).putNumber("frame", 5);
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.SUM_X, projectId, query);

        JsonObject jsonObject = new JsonObject().putNumber("value0", 40667857876800L).putNumber("value1", 40667857905600L).putNumber("value2", 40667857934400L);
        JsonObject json = new JsonObject().putObject(interval.previous().current() + "000", jsonObject);
        for (int i = 0; i < 4; i++)
            json.putObject(interval.previous().current() + "000", new JsonObject());

        assertEquals(json, data.getObject("result"));
    }

    @Test
    public void testAverageXAggregation_whenCountX() {
        String projectId = randomString(10);
        SpanTime interval = fromString("1day").spanCurrent();
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, AVERAGE_X, interval.period, new SimpleFieldScript<String>("test"), null, new SimpleFieldScript<String>("key1"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        int start = interval.current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            JsonObject m = new JsonObject().putNumber("test", i).putString("key1", "value" + i % 3);
            eventAggregator.aggregate(projectId, m, "actor" + i, i);
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson()).putNumber("frame", 5);
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.COUNT_X, projectId, query);

        long count = (next-start)/3;
        JsonObject jsonObject = new JsonObject().putNumber("value0", count).putNumber("value1", count).putNumber("value2", count);
        JsonObject json = new JsonObject().putObject(interval.previous().current() + "000", jsonObject);
        for (int i = 0; i < 4; i++)
            json.putObject(interval.previous().current() + "000", new JsonObject());

        assertEquals(json, data.getObject("result"));
    }

    @Test
    public void testUniqueXAggregation_countUniqueX() {
        String projectId = randomString(10);
        SpanTime interval = fromString("1day").spanCurrent();
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, UNIQUE_X, interval.period, new SimpleFieldScript<String>("test"), null, new SimpleFieldScript<String>("key1"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        int start = interval.current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            JsonObject m = new JsonObject().putString("test", "value"+i).putString("key1", "value" + i % 3);
            eventAggregator.aggregate(projectId, m, "actor" + i, i);
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson()).putNumber("frame", 5);
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.COUNT_UNIQUE_X, projectId, query);

        JsonObject jsonObject = new JsonObject();
        long count = (next-start)/3;
        for (int i = 0; i < 3; i++) {
            jsonObject.putNumber("value"+i, count);
        }
        JsonObject json = new JsonObject().putObject(interval.previous().current() + "000", jsonObject);
        for (int i = 0; i < 4; i++)
            json.putObject(interval.previous().current() + "000", new JsonObject());

        assertEquals(json, data.getObject("result"));
    }

    @Test
    public void testUniqueXAggregation_selectUniqueX() {
        String projectId = randomString(10);
        SpanTime interval = fromString("1day").spanCurrent();
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, UNIQUE_X, interval.period, new SimpleFieldScript<String>("test"), null, new SimpleFieldScript<String>("key1"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        int start = interval.current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            JsonObject m = new JsonObject().putString("test", "value"+i % 3).putString("key1", "value" + i % 2);
            eventAggregator.aggregate(projectId, m, "actor" + i, i);
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson()).putNumber("frame", 5);
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.SELECT_UNIQUE_X, projectId, query);
        JsonObject result = data.getObject("result");
        assertNotNull(result);

        JsonObject json = result.getObject(interval.previous().current() + "000");

        for (int i = 0; i < 4; i++)
            assertEquals(result.getObject(interval.previous().current() + "000"), new JsonObject());

        for (long i = 0; i < 2; i++) {
            JsonArray array = json.getArray("value" + i);
            for (int i1 = 0; i1 < 3; i1++) {
                assertTrue(array.contains("value" + i1));
            }
            assertEquals(array.size(), 3);
        }
    }

    @Before
    public void clear() {
        localStorageAdapter.flush();
        cacheAdapter.flush();
        databaseAdapter.flush();
        DistributedAnalysisRuleMap.clear();
    }
}
