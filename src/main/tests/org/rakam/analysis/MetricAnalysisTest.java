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
import static org.rakam.util.DateUtil.UTCTime;

/**
 * Created by buremba <Burak Emre Kabakcı> on 19/09/14 13:57.
 */
public class MetricAnalysisTest extends RakamTestHelper {
    LocalCacheAdapter localStorageAdapter = new LocalCacheAdapter();
    DistributedCacheAdapter cacheAdapter = new FakeDistributedCacheAdapter();
    DatabaseAdapter databaseAdapter = new DummyDatabase();
    EventAggregator eventAggregator = new EventAggregator(localStorageAdapter, cacheAdapter, databaseAdapter);
    PeriodicCollector collector = new PeriodicCollector(localStorageAdapter, cacheAdapter, databaseAdapter);
    EventAnalyzer eventAnalyzer = new EventAnalyzer(cacheAdapter, databaseAdapter);

    @Test
    public void testCountAggregation() {
//        Injector injector = Guice.createInjector(new AbstractModule() {
//            @Override
//            protected void configure() {
//                bind(DatabaseAdapter.class).to(DummyDatabase.class);
//                bind(CacheAdapter.class).to(LocalCacheAdapter.class);
//                bind(AnalysisRuleDatabase.class).to(DummyDatabase.class);
//            }
//        });

        String projectId = randomString(10);
        MetricAggregationRule rule = new MetricAggregationRule(projectId, AggregationType.COUNT);
        DistributedAnalysisRuleMap.add(projectId, rule);

        for (long i = 0; i < 10000; i++)
            eventAggregator.aggregate(projectId, iterativeJson(5, "key", "value"), "actor" + i, UTCTime());

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson());
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.COUNT, projectId, query);
        assertEqualsFunction(10000L, () -> data.getLong("result"), data);
    }

    @Test
    public void testCountXAggregation() {
        String projectId = randomString(10);
        MetricAggregationRule rule = new MetricAggregationRule(projectId, AggregationType.COUNT_X, new SimpleFieldScript<String>("key2"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        for (long i = 0; i < 10000; i++)
            eventAggregator.aggregate(projectId, iterativeJson(2, "key", "value"), "actor" + i, UTCTime());

        for (long i = 0; i < 10000; i++)
            eventAggregator.aggregate(projectId, iterativeJson(3, "key", "value"), "actor" + i, UTCTime());

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson());
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.COUNT_X, projectId, query);
        assertEqualsFunction(10000L, () -> data.getLong("result").longValue(), data);
    }


    @Test
    public void testSumXAggregation() {
        String projectId = randomString(10);
        MetricAggregationRule rule = new MetricAggregationRule(projectId, AggregationType.SUM_X, new SimpleFieldScript<String>("test"));
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
    public void testMaxXAggregation() {
        String projectId = randomString(10);
        MetricAggregationRule rule = new MetricAggregationRule(projectId, AggregationType.MAXIMUM_X, new SimpleFieldScript<String>("test"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        for (long i = 0; i <= 10000; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().putNumber("test", i), "actor" + i, UTCTime());
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson());
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.MAXIMUM_X, projectId, query);
        assertEqualsFunction(10000L, () -> data.getLong("result").longValue(), data);
    }


    @Test
    public void testMinXAggregation() {
        String projectId = randomString(10);
        MetricAggregationRule rule = new MetricAggregationRule(projectId, AggregationType.MINIMUM_X, new SimpleFieldScript<String>("test"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        for (long i = 10000; i < 20000; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().putNumber("test", i), "actor" + i, UTCTime());
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson());
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.MINIMUM_X, projectId, query);
        assertEqualsFunction(10000L, () -> data.getLong("result"), data);
    }


    @Test
    public void testAverageXAggregation() {
        String projectId = randomString(10);
        MetricAggregationRule rule = new MetricAggregationRule(projectId, AggregationType.AVERAGE_X, new SimpleFieldScript<String>("test"));
        DistributedAnalysisRuleMap.add(projectId, rule);

        for (long i = 10000; i < 20000; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().putNumber("test", i), "actor" + i, UTCTime());
        }

        DistributedAnalysisRuleMap.entrySet().forEach(collector::process);

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson());
        JsonObject data = eventAnalyzer.analyze(AggregationAnalysis.AVERAGE_X, projectId, query);
        assertEqualsFunction(((long) ((10000 + 19999) / 2)), () -> data.getLong("result").longValue(), data);
    }


    @Test
    public void testAverageXAggregation_whenSumX() {
        String projectId = randomString(10);
        MetricAggregationRule rule = new MetricAggregationRule(projectId, AggregationType.AVERAGE_X, new SimpleFieldScript<String>("test"));
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
        MetricAggregationRule rule = new MetricAggregationRule(projectId, AggregationType.AVERAGE_X, new SimpleFieldScript<String>("test"));
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
