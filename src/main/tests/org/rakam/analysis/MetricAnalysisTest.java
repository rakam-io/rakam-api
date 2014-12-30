package org.rakam.analysis;

import org.junit.Test;
import org.rakam.analysis.query.simple.SimpleFieldScript;
import org.rakam.analysis.rule.aggregation.MetricAggregationRule;
import org.rakam.constant.AggregationAnalysis;
import org.rakam.constant.AggregationType;
import org.rakam.util.json.JsonArray;
import org.rakam.util.json.JsonObject;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.rakam.util.TimeUtil.UTCTime;

/**
 * Created by buremba <Burak Emre Kabakcı> on 19/09/14 13:57.
 */
public class MetricAnalysisTest extends AnalysisBaseTest {

    @Test
    public void testCountAggregation() {

        String projectId = randomString(10);
        MetricAggregationRule rule = new MetricAggregationRule(projectId, AggregationType.COUNT);
        analysisRuleMap.add(projectId, rule);

        for (long i = 0; i < 10000; i++)
            eventAggregator.aggregate(projectId, iterativeJson(5, "key", "value"), "actor" + i, UTCTime());

        collector.process(analysisRuleMap.entrySet());

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson())
                .putString("analysis_type", AggregationAnalysis.COUNT.name());
        JsonObject data = eventAnalyzer.analyze(query);
        assertEqualsFunction(10000L, () -> data.getLong("result"), data);
    }

    @Test
    public void testCountXAggregation() {
        String projectId = randomString(10);
        MetricAggregationRule rule = new MetricAggregationRule(projectId, AggregationType.COUNT_X, new SimpleFieldScript<String>("key2"));
        analysisRuleMap.add(projectId, rule);

        for (long i = 0; i < 10000; i++)
            eventAggregator.aggregate(projectId, iterativeJson(2, "key", "value"), "actor" + i, UTCTime());

        for (long i = 0; i < 10000; i++)
            eventAggregator.aggregate(projectId, iterativeJson(3, "key", "value"), "actor" + i, UTCTime());

        collector.process(analysisRuleMap.entrySet());

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson())
                .putString("analysis_type", AggregationAnalysis.COUNT_X.name());
        JsonObject data = eventAnalyzer.analyze(query);
        assertEqualsFunction(10000L, () -> data.getLong("result").longValue(), data);
    }


    @Test
    public void testSumXAggregation() {
        String projectId = randomString(10);
        MetricAggregationRule rule = new MetricAggregationRule(projectId, AggregationType.SUM_X, new SimpleFieldScript<String>("test"));
        analysisRuleMap.add(projectId, rule);

        for (long i = 0; i <= 10000; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().putNumber("test", i), "actor" + i, UTCTime());
        }

        collector.process(analysisRuleMap.entrySet());

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson())
                .putString("analysis_type", AggregationAnalysis.SUM_X.name());
        JsonObject data = eventAnalyzer.analyze(query);
        assertEqualsFunction(((long) ((1 + 10000) * (10000 / 2))), () -> data.getLong("result").longValue(), data);
    }


    @Test
    public void testMaxXAggregation() {
        String projectId = randomString(10);
        MetricAggregationRule rule = new MetricAggregationRule(projectId, AggregationType.MAXIMUM_X, new SimpleFieldScript<String>("test"));
        analysisRuleMap.add(projectId, rule);

        for (long i = 0; i <= 10000; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().putNumber("test", i), "actor" + i, UTCTime());
        }

        collector.process(analysisRuleMap.entrySet());

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson())
                .putString("analysis_type", AggregationAnalysis.MAXIMUM_X.name());
        JsonObject data = eventAnalyzer.analyze(query);
        assertEqualsFunction(10000L, () -> data.getLong("result").longValue(), data);
    }


    @Test
    public void testMinXAggregation() {
        String projectId = randomString(10);
        MetricAggregationRule rule = new MetricAggregationRule(projectId, AggregationType.MINIMUM_X, new SimpleFieldScript<String>("test"));
        analysisRuleMap.add(projectId, rule);

        for (long i = 10000; i < 20000; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().putNumber("test", i), "actor" + i, UTCTime());
        }

        collector.process(analysisRuleMap.entrySet());

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson())
                .putString("analysis_type", AggregationAnalysis.MINIMUM_X.name());
        JsonObject data = eventAnalyzer.analyze(query);
        assertEqualsFunction(10000L, () -> data.getLong("result"), data);
    }


    @Test
    public void testAverageXAggregation() {
        String projectId = randomString(10);
        MetricAggregationRule rule = new MetricAggregationRule(projectId, AggregationType.AVERAGE_X, new SimpleFieldScript<String>("test"));
        analysisRuleMap.add(projectId, rule);

        for (long i = 10000; i < 20000; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().putNumber("test", i), "actor" + i, UTCTime());
        }

        collector.process(analysisRuleMap.entrySet());

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson())
                .putString("analysis_type", AggregationAnalysis.AVERAGE_X.name());
        JsonObject data = eventAnalyzer.analyze(query);
        assertEqualsFunction(((long) ((10000 + 19999) / 2)), () -> data.getLong("result").longValue(), data);
    }


    @Test
    public void testAverageXAggregation_whenSumX() {
        String projectId = randomString(10);
        MetricAggregationRule rule = new MetricAggregationRule(projectId, AggregationType.AVERAGE_X, new SimpleFieldScript<String>("test"));
        analysisRuleMap.add(projectId, rule);

        for (long i = 0; i <= 10000; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().putNumber("test", i), "actor" + i, UTCTime());
        }

        collector.process(analysisRuleMap.entrySet());

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson())
                .putString("analysis_type", AggregationAnalysis.SUM_X.name());
        JsonObject data = eventAnalyzer.analyze(query);
        assertEqualsFunction(((long) ((1 + 10000) * (10000 / 2))), () -> data.getLong("result").longValue(), data);
    }

    @Test
    public void testAverageXAggregation_whenCountX() {
        String projectId = randomString(10);
        MetricAggregationRule rule = new MetricAggregationRule(projectId, AggregationType.AVERAGE_X, new SimpleFieldScript<String>("test"));
        analysisRuleMap.add(projectId, rule);

        for (long i = 0; i < 10000; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().putNumber("test", i), "actor" + i, UTCTime());
        }

        collector.process(analysisRuleMap.entrySet());

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson())
                .putString("analysis_type", AggregationAnalysis.COUNT_X.name());
        JsonObject data = eventAnalyzer.analyze(query);
        assertEqualsFunction(10000L, () -> data.getLong("result").longValue(), data);
    }

    @Test
    public void testUniqueXAggregation_countUniqueX() {
        String projectId = randomString(10);
        MetricAggregationRule rule = new MetricAggregationRule(projectId, AggregationType.UNIQUE_X, new SimpleFieldScript<String>("test"));
        analysisRuleMap.add(projectId, rule);

        for (long i = 0; i < 10000; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().putNumber("test", i%100), "actor" + i, UTCTime());
        }

        collector.process(analysisRuleMap.entrySet());

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson())
                .putString("analysis_type", AggregationAnalysis.COUNT_UNIQUE_X.name());
        JsonObject data = eventAnalyzer.analyze(query);
        assertEqualsFunction(100, () -> data.getNumber("result"), data);
    }

    @Test
    public void testUniqueXAggregation_selectUniqueX() {
        String projectId = randomString(10);
        MetricAggregationRule rule = new MetricAggregationRule(projectId, AggregationType.UNIQUE_X, new SimpleFieldScript<String>("test"));
        analysisRuleMap.add(projectId, rule);

        for (long i = 0; i < 10000; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().putString("test", "test" + (i % 100)), "actor" + i, UTCTime());
        }

        collector.process(analysisRuleMap.entrySet());

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson())
                .putString("analysis_type", AggregationAnalysis.SELECT_UNIQUE_X.name())
                .putNumber("items", 100);
        JsonObject data = eventAnalyzer.analyze(query);
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
        analysisRuleMap.add(projectId, rule);

        for (int i = 0; i < 100; i++) {
            JsonObject map = new JsonObject();
            map.putString("test", "value" + (i % 10));
            databaseAdapter.createActor(projectId, "actor" + i, map);
        }

        for (long i = 0; i < 10000; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().putString("actor", "actor" + (i % 100)), "actor" + i, UTCTime());
        }

        collector.process(analysisRuleMap.entrySet());

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson())
                .putString("analysis_type", AggregationAnalysis.SELECT_UNIQUE_X.name());
        JsonObject data = eventAnalyzer.analyze(query);
        JsonArray result = data.getArray("result");
        assertNotNull(result);

        for (long i = 0; i < 10; i++) {
            assertTrue(result.contains("value" + i));
        }
        assertEquals(10, result.size());
    }

}
