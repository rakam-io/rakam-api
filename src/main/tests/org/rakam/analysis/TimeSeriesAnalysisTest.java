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
import org.rakam.util.Interval;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.HashMap;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.rakam.constant.AggregationType.AVERAGE_X;
import static org.rakam.constant.AggregationType.COUNT;
import static org.rakam.constant.AggregationType.COUNT_X;
import static org.rakam.constant.AggregationType.MAXIMUM_X;
import static org.rakam.constant.AggregationType.MINIMUM_X;
import static org.rakam.constant.AggregationType.SUM_X;
import static org.rakam.constant.AggregationType.UNIQUE_X;

/**
 * Created by buremba <Burak Emre Kabakcı> on 19/09/14 13:57.
 */
public class TimeSeriesAnalysisTest extends RakamTestHelper {
    LocalCacheAdapter localStorageAdapter = new LocalCacheAdapter();
    DistributedCacheAdapter cacheAdapter = new FakeDistributedCacheAdapter();
    DatabaseAdapter databaseAdapter = new DummyDatabase();
    EventAggregator eventAggregator = new EventAggregator(localStorageAdapter, cacheAdapter, databaseAdapter);
    PeriodicCollector collector = new PeriodicCollector(localStorageAdapter, cacheAdapter, databaseAdapter);
    EventAnalyzer eventAnalyzer = new EventAnalyzer(cacheAdapter, databaseAdapter);

    private static final Interval ONE_DAY = Interval.parse("1day");
    
    @Test
    public void testCountAggregation() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent();

        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, COUNT, ONE_DAY);
        DistributedAnalysisRuleMap.add(projectId, rule);

        int start = interval.current();
        int next = interval.next().current();
        for (int i = start; i < next; i++) {
            JsonObject m = iterativeJson(3, "key", "value");
            eventAggregator.aggregate(projectId, m, "actor" + i, i);
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson()).putNumber("frame", 5);
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.COUNT, projectId, query);

        JsonObject json = new JsonObject().putNumber(interval.previous().current() + "000", ((long) (next - start)));
        for (int i = 0; i < 4; i++)
            json.putNumber(interval.previous().current()+"000", 0);

        assertEquals(data.getObject("result"), json);
    }

    @Test
    public void testCountXAggregation() {
        String projectId = randomString(10);
Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent(); 

        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, COUNT_X, ONE_DAY, new SimpleFieldScript<String>("key2"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        int start = interval.current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            eventAggregator.aggregate(projectId, iterativeJson(2, "key", "value"), "actor" + i, i);
            eventAggregator.aggregate(projectId, iterativeJson(3, "key", "value"), "actor" + i, i);
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson()).putNumber("frame", 5);
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.COUNT_X, projectId, query);

        JsonObject json = new JsonObject().putNumber(interval.previous().current() + "000", ((long) (next - start)));
        for (int i = 0; i < 4; i++)
            json.putNumber(interval.previous().current()+"000", 0);

        assertEquals(data.getObject("result"), json);
    }


    @Test
    public void testSumXAggregation() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent(); 

        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, SUM_X, ONE_DAY, new SimpleFieldScript<String>("test"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        int start = interval.current();
        int next = interval.next().current();

        long sum = 0;
        for (int i = start; i < next; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().putNumber("test", i), "actor" + i, i);
            sum += i;
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson()).putNumber("frame", 5);
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.SUM_X, projectId, query);

        JsonObject json = new JsonObject().putNumber(interval.previous().current() + "000", sum);
        for (int i = 0; i < 4; i++)
            json.putNumber(interval.previous().current()+"000", 0);

        assertEquals(data.getObject("result"), json);
    }


    @Test
    public void testMaxXAggregation() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent(); 

        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, MAXIMUM_X, ONE_DAY, new SimpleFieldScript<String>("test"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        int start = interval.current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().putNumber("test", i), "actor" + i, i);
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson()).putNumber("frame", 5);
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.MAXIMUM_X, projectId, query);

        JsonObject json = new JsonObject().putNumber(interval.previous().current() + "000", next - 1L);
        for (int i = 0; i < 4; i++)
            json.putNumber(interval.previous().current()+"000", 0);

        assertEquals(data.getObject("result"), json);
    }


    @Test
    public void testMinXAggregation() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent(); 

        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, MINIMUM_X, ONE_DAY, new SimpleFieldScript<String>("test"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        int start = interval.current();
        int next = interval.next().current();

        for (int i = start; i <= next; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().putNumber("test", i), "actor" + i, i);
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson()).putNumber("frame", 5);
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.MINIMUM_X, projectId, query);

        JsonObject json = new JsonObject().putNumber(interval.previous().current() + "000", ((long) start));
        for (int i = 0; i < 4; i++)
            json.putNumber(interval.previous().current()+"000", 0);

        assertEquals(data.getObject("result"), json);
    }


    @Test
    public void testAverageXAggregation() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent(); 

        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, AVERAGE_X, ONE_DAY, new SimpleFieldScript<String>("test"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        int start = interval.current();
        int next = interval.next().current();

        for (int i = start; i <= next; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().putNumber("test", i), "actor" + i, i);
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson()).putNumber("frame", 5);
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.AVERAGE_X, projectId, query);

        JsonObject json = new JsonObject().putNumber(interval.previous().current() + "000", (((long) next-1) + ((long) start)) / 2);
        for (int i = 0; i < 4; i++)
            json.putNumber(interval.previous().current()+"000", 0L);

        assertEquals(data.getObject("result"), json);
    }


    @Test
    public void testAverageXAggregation_whenSumX() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent(); 

        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, AVERAGE_X, ONE_DAY, new SimpleFieldScript<String>("test"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        int start = interval.current();
        int next = interval.next().current();

        long sum = 0;
        for (int i = start; i < next; i++) {
            sum += i;
            eventAggregator.aggregate(projectId, new JsonObject().putNumber("test", i), "actor" + i, i);
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson()).putNumber("frame", 5);
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.SUM_X, projectId, query);

        JsonObject json = new JsonObject().putNumber(interval.previous().current() + "000", sum);
        for (int i = 0; i < 4; i++)
            json.putNumber(interval.previous().current()+"000", 0L);

        assertEquals(data.getObject("result"), json);
    }

    @Test
    public void testAverageXAggregation_whenCountX() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent(); 

        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, AVERAGE_X, ONE_DAY, new SimpleFieldScript<String>("test"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        int start = interval.current();
        int next = interval.next().current();

        for (int i = start; i <= next; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().putNumber("test", i), "actor" + i, i);
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson()).putNumber("frame", 5);
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.COUNT_X, projectId, query);

        JsonObject json = new JsonObject().putNumber(interval.previous().current() + "000", ((long) (next - start)));
        for (int i = 0; i < 4; i++)
            json.putNumber(interval.previous().current()+"000", 0L);

        assertEquals(data.getObject("result"), json);
    }

    @Test
    public void testUniqueXAggregation_countUniqueX() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent(); 

        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, UNIQUE_X, ONE_DAY, new SimpleFieldScript<String>("test"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        int start = interval.current();
        int next = interval.next().current();

        for (int i = start; i <= next; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().putNumber("test", i % 100), "actor" + i, i);
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson()).putNumber("frame", 5);
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.COUNT_UNIQUE_X, projectId, query);

        JsonObject json = new JsonObject().putNumber(interval.previous().current() + "000", 100);
        for (int i = 0; i < 4; i++)
            json.putNumber(interval.previous().current() + "000", 0);

        assertEquals(data.getObject("result"), json);
    }

    @Test
    public void testUniqueXAggregation_selectUniqueX() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent(); 

        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, UNIQUE_X, ONE_DAY, new SimpleFieldScript<String>("test"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        int start = interval.current();
        int next =  ONE_DAY.spanCurrent().next().current();

        for (int i = start; i <= next; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().putString("test", "test" + (i % 100)), "actor" + i, i);
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson()).putNumber("frame", 5);
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.SELECT_UNIQUE_X, projectId, query).getObject("result");

        JsonArray array = data.getArray(interval.current() + "000");
        for (long i = 0; i < 100; i++)
            assertTrue(array.contains("test" + i));

        assertEquals(100, array.size());

        for (int i = 0; i < 4; i++)
            assertEquals(data.getArray(interval.previous().current() + "000"), new JsonArray());
    }

    @Test
    public void testUniqueXAggregation_attributeBelongsUser() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent(); 

        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, UNIQUE_X, ONE_DAY, new SimpleFieldScript<String>("_user.test"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        for (int i = 0; i < 100; i++) {
            HashMap<String, Object> map = new HashMap<>();
            map.put("test", "value" + (i % 10));
            databaseAdapter.createActor(projectId, "actor" + i, map);
        }

        int start = interval.current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            eventAggregator.aggregate(projectId, new JsonObject(), "actor" + (i % 100), i);
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson()).putNumber("frame", 5);
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.SELECT_UNIQUE_X, projectId, query);

        JsonArray items = new JsonArray();
        for (long i = 0; i < 10; i++) {
            items.addString("value" + i);
        }

        JsonObject json = new JsonObject().putArray(interval.previous().current() + "000", items);
        for (int i = 0; i < 4; i++)
            json.putArray(interval.previous().current() + "000", new JsonArray());

        assertEquals(data.getObject("result"), json);
    }

    @Test
    public void testInterval_whenStartEndTimestamp() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent(); 

        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, COUNT, ONE_DAY);
        DistributedAnalysisRuleMap.add(projectId, rule);

        int start = interval.current();

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson())
                .putNumber("end", start)
                .putNumber("start", ONE_DAY.spanCurrent().previous().previous().previous().previous().current());
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.COUNT, projectId, query);

        JsonObject json = new JsonObject();
        for (int i = 0; i < 5; i++) {
            json.putNumber(interval.current() + "000", 0);
            interval.previous();
        }

        assertEquals(data.getObject("result"), json);
    }

    @Test
    public void testInterval_whenStartEndExceeds() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent(); 

        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, COUNT, ONE_DAY);
        DistributedAnalysisRuleMap.add(projectId, rule);

        int start = interval.current();


        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson())
                .putNumber("end", start)
                .putNumber("start", 0);
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.COUNT, projectId, query);

        assertEquals(data, new JsonObject().putString("error", "there is more than 100 items between start and times."));
    }

    @Test
    public void testInterval_whenFrameExceeds() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent(); 

        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, COUNT, ONE_DAY);
        DistributedAnalysisRuleMap.add(projectId, rule);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson())
                .putNumber("frame", 10000);
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.COUNT, projectId, query);

        assertEquals(data, new JsonObject().putString("error", "items must be lower than 100"));
    }

    @Before
    public void clear() {
        localStorageAdapter.flush();
        cacheAdapter.flush();
        databaseAdapter.flush();
        DistributedAnalysisRuleMap.clear();
    }
}
