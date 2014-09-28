package org.rakam.analysis;

import org.junit.Before;
import org.junit.Test;
import org.rakam.RakamTestHelper;
import org.rakam.analysis.query.simple.SimpleFieldScript;
import org.rakam.analysis.rule.aggregation.MetricAggregationRule;
import org.rakam.cache.DistributedAnalysisRuleMap;
import org.rakam.cache.DistributedCacheAdapter;
import org.rakam.cache.local.LocalCacheAdapter;
import org.rakam.collection.EventAggregator;
import org.rakam.collection.PeriodicCollector;
import org.rakam.constant.AggregationAnalysis;
import org.rakam.constant.AggregationType;
import org.rakam.database.DatabaseAdapter;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.HashMap;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.rakam.constant.AggregationType.AVERAGE_X;
import static org.rakam.constant.AggregationType.MINIMUM_X;
import static org.rakam.util.DateUtil.UTCTime;

/**
 * Created by buremba <Burak Emre Kabakcı> on 23/09/14 21:58.
 */
public class GroupingMetricAnalysisTest extends RakamTestHelper {
    LocalCacheAdapter localStorageAdapter = new LocalCacheAdapter();
    DistributedCacheAdapter cacheAdapter = new FakeDistributedCacheAdapter();
    DatabaseAdapter databaseAdapter = new DummyDatabase();
    EventAggregator eventAggregator = new EventAggregator(localStorageAdapter, cacheAdapter, databaseAdapter);
    PeriodicCollector collector = new PeriodicCollector(localStorageAdapter, cacheAdapter, databaseAdapter);
    EventAnalyzer eventAnalyzer = new EventAnalyzer(cacheAdapter, databaseAdapter);

    @Test
    public void testCountAggregation() {
        String projectId = randomString(10);
        MetricAggregationRule rule = new MetricAggregationRule(projectId, AggregationType.COUNT, null, null, new SimpleFieldScript<String>("key"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        for (long i = 0; i < 10000; i++)
            eventAggregator.aggregate(projectId, new JsonObject().putString("key", "value" + i % 5), "actor" + i, UTCTime());

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson());
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.COUNT, projectId, query);

        JsonObject json = new JsonObject();
        for (int i = 0; i < 5; i++) {
            json.putNumber("value"+i, 2000L);
        }
        assertEquals(new JsonObject().putObject("result", json), data);
    }

    @Test
    public void testCountXAggregation() {
        String projectId = randomString(10);
        MetricAggregationRule rule = new MetricAggregationRule(projectId, AggregationType.COUNT_X, new SimpleFieldScript<String>("key2"), null, new SimpleFieldScript<String>("key1"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        for (long i = 0; i < 10000; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().putString("key1", "value" + i % 5).putString("key2", "value" + (i+1) % 5), "actor" + i, UTCTime());
            eventAggregator.aggregate(projectId, new JsonObject().putString("key1", "value" + i % 5), "actor" + i, UTCTime());
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson());
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.COUNT_X, projectId, query);

        JsonObject json = new JsonObject();
        for (int i = 0; i < 5; i++) {
            json.putNumber("value"+i, 4000L);
        }
        assertEquals(new JsonObject().putObject("result", json), data);    }


    @Test
    public void testSumXAggregation() {
        String projectId = randomString(10);
        MetricAggregationRule rule = new MetricAggregationRule(projectId, AggregationType.SUM_X, new SimpleFieldScript<String>("key2"), null, new SimpleFieldScript<String>("key1"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        for (long i = 0; i < 10000; i++) {
            JsonObject m = new JsonObject().putString("key1", "value" + i % 5).putNumber("key2", (i + 1) % 5);
            eventAggregator.aggregate(projectId, m, "actor" + i, UTCTime());
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson());
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.SUM_X, projectId, query);

        JsonObject jsonObject = new JsonObject()
                .putNumber("value0", 2000L)
                .putNumber("value2", 6000L)
                .putNumber("value1", 4000L)
                .putNumber("value4", 0L)
                .putNumber("value3", 8000L);
        assertEquals(data, new JsonObject().putObject("result",jsonObject));
    }


    @Test
    public void testMaxXAggregation() {
        String projectId = randomString(10);
        MetricAggregationRule rule = new MetricAggregationRule(projectId, AggregationType.MAXIMUM_X, new SimpleFieldScript<String>("test"),null, new SimpleFieldScript<String>("key1"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        for (long i = 0; i < 10000; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().putNumber("test", i).putString("key1", "value"+i%5), "actor" + i, UTCTime());
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson());
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.MAXIMUM_X, projectId, query);

        JsonObject jsonObject = new JsonObject();
        for (long i = 0; i < 5; i++) {
            jsonObject.putNumber("value"+i, 2000L);
        }

        assertEquals(data, new JsonObject().putObject("result",jsonObject));
    }


    @Test
    public void testMinXAggregation() {
        String projectId = randomString(10);
        MetricAggregationRule rule = new MetricAggregationRule(projectId, MINIMUM_X, new SimpleFieldScript<String>("test"), null, new SimpleFieldScript<String>("key1"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        for (long i = 10000; i < 20000; i++) {
            JsonObject m = new JsonObject().putNumber("test", 10).putString("key1", "value" + i % 10);
            eventAggregator.aggregate(projectId, m, "actor" + i, UTCTime());
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson());
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.MINIMUM_X, projectId, query);
        JsonObject jsonObject = new JsonObject();
        for (int i = 0; i < 10; i++) {
            jsonObject.putNumber("value"+i, 1000L);
        };
        assertEquals(data, new JsonObject().putObject("result",jsonObject));
    }


    @Test
    public void testAverageXAggregation() {
        String projectId = randomString(10);
        MetricAggregationRule rule = new MetricAggregationRule(projectId, AVERAGE_X, new SimpleFieldScript<String>("test"), null, new SimpleFieldScript<String>("key1"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        for (long i = 0; i < 20000; i++) {
            JsonObject m = new JsonObject().putNumber("test", i).putString("key1", "value" + i % 10);;
            eventAggregator.aggregate(projectId, m, "actor" + i, UTCTime());
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson());
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.AVERAGE_X, projectId, query);
        JsonObject jsonObject = new JsonObject();
        for (int i = 0; i < 10; i++) {

            jsonObject.putNumber("value"+i, 9995L+i);
        }

        assertEquals(data, new JsonObject().putObject("result", jsonObject));
    }


    @Test
    public void testAverageXAggregation_whenSumX() {
        String projectId = randomString(10);
        MetricAggregationRule rule = new MetricAggregationRule(projectId, AVERAGE_X, new SimpleFieldScript<String>("test"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        for (long i = 0; i <= 10000; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().putNumber("test", i), "actor" + i, UTCTime());
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson());
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.SUM_X, projectId, query);
        assertEqualsFunction(((long) ((1 + 10000) * (10000 / 2))), () -> data.getLong("result").longValue(), data);
    }

    @Test
    public void testAverageXAggregation_whenCountX() {
        String projectId = randomString(10);
        MetricAggregationRule rule = new MetricAggregationRule(projectId, AVERAGE_X, new SimpleFieldScript<String>("test"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        for (long i = 0; i < 10000; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().putNumber("test", i), "actor" + i, UTCTime());
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson());
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.COUNT_X, projectId, query);
        assertEqualsFunction(10000L, () -> data.getLong("result").longValue(), data);
    }

    @Test
    public void testUniqueXAggregation_countUniqueX() {
        String projectId = randomString(10);
        MetricAggregationRule rule = new MetricAggregationRule(projectId, AggregationType.UNIQUE_X, new SimpleFieldScript<String>("test"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        for (long i = 0; i < 10000; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().putNumber("test", i%100), "actor" + i, UTCTime());
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson());
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.COUNT_UNIQUE_X, projectId, query);
        assertEqualsFunction(100, () -> data.getNumber("result"), data);
    }

    @Test
    public void testUniqueXAggregation_selectUniqueX() {
        String projectId = randomString(10);
        MetricAggregationRule rule = new MetricAggregationRule(projectId, AggregationType.UNIQUE_X, new SimpleFieldScript<String>("test"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        for (long i = 0; i < 10000; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().putString("test", "test" + (i % 100)), "actor" + i, UTCTime());
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson());
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.SELECT_UNIQUE_X, projectId, query);
        JsonArray result = data.getArray("result");
        assertNotNull(result);

        for (long i = 0; i < 100; i++) {
            assertTrue(result.contains("test" + i));
        }
        assertEquals(100, result.size());
    }

    @Test
    public void testUniqueXAggregation_attributeBelongsUser() {
        String projectId = randomString(10);
        MetricAggregationRule rule = new MetricAggregationRule(projectId, AggregationType.UNIQUE_X, new SimpleFieldScript<String>("_user.test"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        for (int i = 0; i < 100; i++) {
            HashMap<String, Object> map = new HashMap<>();
            map.put("test", "value" + (i % 10));
            databaseAdapter.createActor(projectId, "actor" + i, map);
        }

        for (long i = 0; i < 10000; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().putString("actor", "actor" + (i % 100)), "actor" + i, UTCTime());
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson());
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.SELECT_UNIQUE_X, projectId, query);
        JsonArray result = data.getArray("result");
        assertNotNull(result);

        for (long i = 0; i < 10; i++) {
            assertTrue(result.contains("value" + i));
        }
        assertEquals(10, result.size());
    }

    @Before
    public void clear() {
        localStorageAdapter.flush();
        cacheAdapter.flush();
        databaseAdapter.flush();
        DistributedAnalysisRuleMap.clear();
    }
}
