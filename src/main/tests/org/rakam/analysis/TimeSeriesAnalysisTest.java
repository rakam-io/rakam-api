package org.rakam.analysis;

import org.junit.Test;
import org.rakam.analysis.query.simple.SimpleFieldScript;
import org.rakam.analysis.rule.aggregation.TimeSeriesAggregationRule;
import org.rakam.constant.AggregationAnalysis;
import org.rakam.util.Interval;
import org.rakam.util.json.JsonArray;
import org.rakam.util.json.JsonObject;

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

/**
 * Created by buremba <Burak Emre Kabakcı> on 19/09/14 13:57.
 */
public class TimeSeriesAnalysisTest extends AnalysisBaseTest {
    private static final Interval ONE_DAY = Interval.parse("1days");
    
    @Test
    public void testCountAggregation() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent();

        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, COUNT, ONE_DAY);
        analysisRuleMap.add(rule);

        int start = interval.current();
        int next = interval.next().current();
        for (int i = start; i < next; i++) {
            JsonObject m = iterativeJson(3, "key", "value");
            eventAggregator.aggregate(projectId, m, "actor" + i, i);
        }

        //collector.process(analysisRuleMap.entrySet());

        JsonObject query = new JsonObject().put("tracker", projectId).mergeIn(rule.toJson()).put("frame", 5)
                .put("analysis_type", AggregationAnalysis.COUNT.name());
        JsonObject data = eventAnalyzer.analyze(query);

        JsonObject json = new JsonObject().put(formatTime(interval.previous().current()), ((long) (next - start)));
        for (int i = 0; i < 4; i++)
            json.put(formatTime(interval.previous().current()), 0);

        assertEquals(data.getJsonObject("result"), json);
    }

    @Test
    public void testCountXAggregation() {
        String projectId = randomString(10);
Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent(); 

        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, COUNT_X, ONE_DAY, new SimpleFieldScript<String>("key2"));
        analysisRuleMap.add(rule);

        int start = interval.current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            eventAggregator.aggregate(projectId, iterativeJson(2, "key", "value"), "actor" + i, i);
            eventAggregator.aggregate(projectId, iterativeJson(3, "key", "value"), "actor" + i, i);
        }

        //collector.process(analysisRuleMap.entrySet());

        JsonObject query = new JsonObject().put("tracker", projectId).mergeIn(rule.toJson()).put("frame", 5)
                .put("analysis_type", AggregationAnalysis.COUNT_X.name());
        JsonObject data = eventAnalyzer.analyze(query);

        JsonObject json = new JsonObject().put(formatTime(interval.previous().current()), ((long) (next - start)));
        for (int i = 0; i < 4; i++)
            json.put(formatTime(interval.previous().current()), 0);

        assertEquals(data.getJsonObject("result"), json);
    }


    @Test
    public void testSumXAggregation() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent(); 

        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, SUM_X, ONE_DAY, new SimpleFieldScript<String>("test"));
        analysisRuleMap.add(rule);

        int start = interval.current();
        int next = interval.next().current();

        long sum = 0;
        for (int i = start; i < next; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().put("test", i), "actor" + i, i);
            sum += i;
        }

        //collector.process(analysisRuleMap.entrySet());

        JsonObject query = new JsonObject().put("tracker", projectId).mergeIn(rule.toJson()).put("frame", 5)
                .put("analysis_type", AggregationAnalysis.SUM_X.name());
        JsonObject data = eventAnalyzer.analyze(query);

        JsonObject json = new JsonObject().put(formatTime(interval.previous().current()), sum);
        for (int i = 0; i < 4; i++)
            json.put(formatTime(interval.previous().current()), 0);

        assertEquals(data.getJsonObject("result"), json);
    }


    @Test
    public void testMaxXAggregation() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent(); 

        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, MAXIMUM_X, ONE_DAY, new SimpleFieldScript<String>("test"));
        analysisRuleMap.add(rule);

        int start = interval.current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().put("test", i), "actor" + i, i);
        }

        //collector.process(analysisRuleMap.entrySet());

        JsonObject query = new JsonObject().put("tracker", projectId).mergeIn(rule.toJson()).put("frame", 5)
                .put("analysis_type", AggregationAnalysis.MAXIMUM_X.name());
        JsonObject data = eventAnalyzer.analyze(query);

        JsonObject json = new JsonObject().put(formatTime(interval.previous().current()), next - 1L);
        for (int i = 0; i < 4; i++)
            json.put(formatTime(interval.previous().current()), 0);

        assertEquals(data.getJsonObject("result"), json);
    }


    @Test
    public void testMinXAggregation() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent(); 

        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, MINIMUM_X, ONE_DAY, new SimpleFieldScript<String>("test"));
        analysisRuleMap.add(rule);

        int start = interval.current();
        int next = interval.next().current();

        for (int i = start; i <= next; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().put("test", i), "actor" + i, i);
        }

        //collector.process(analysisRuleMap.entrySet());

        JsonObject query = new JsonObject().put("tracker", projectId).mergeIn(rule.toJson()).put("frame", 5)
                .put("analysis_type", AggregationAnalysis.MINIMUM_X.name());
        JsonObject data = eventAnalyzer.analyze(query);

        JsonObject json = new JsonObject().put(formatTime(interval.previous().current()), ((long) start));
        for (int i = 0; i < 4; i++)
            json.put(formatTime(interval.previous().current()), 0);

        assertEquals(data.getJsonObject("result"), json);
    }


    @Test
    public void testAverageXAggregation() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent(); 

        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, AVERAGE_X, ONE_DAY, new SimpleFieldScript<String>("test"));
        analysisRuleMap.add(rule);

        int start = interval.current();
        int next = interval.next().current();

        for (int i = start; i <= next; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().put("test", i), "actor" + i, i);
        }

        //collector.process(analysisRuleMap.entrySet());

        JsonObject query = new JsonObject().put("tracker", projectId).mergeIn(rule.toJson()).put("frame", 5)
                .put("analysis_type", AggregationAnalysis.AVERAGE_X.name());
        JsonObject data = eventAnalyzer.analyze(query);

        JsonObject json = new JsonObject().put(formatTime(interval.previous().current()), (((long) next-1) + ((long) start)) / 2);
        for (int i = 0; i < 4; i++)
            json.put(formatTime(interval.previous().current()), 0L);

        assertEquals(data.getJsonObject("result"), json);
    }


    @Test
    public void testAverageXAggregation_whenSumX() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent(); 

        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, AVERAGE_X, ONE_DAY, new SimpleFieldScript<String>("test"));
        analysisRuleMap.add(rule);

        int start = interval.current();
        int next = interval.next().current();

        long sum = 0;
        for (int i = start; i < next; i++) {
            sum += i;
            eventAggregator.aggregate(projectId, new JsonObject().put("test", i), "actor" + i, i);
        }

        //collector.process(analysisRuleMap.entrySet());

        JsonObject query = new JsonObject().put("tracker", projectId).mergeIn(rule.toJson()).put("frame", 5)
                .put("analysis_type", AggregationAnalysis.SUM_X.name());
        JsonObject data = eventAnalyzer.analyze(query);

        JsonObject json = new JsonObject().put(formatTime(interval.previous().current()), sum);
        for (int i = 0; i < 4; i++)
            json.put(formatTime(interval.previous().current()), 0L);

        assertEquals(data.getJsonObject("result"), json);
    }

    @Test
    public void testAverageXAggregation_whenCountX() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent(); 

        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, AVERAGE_X, ONE_DAY, new SimpleFieldScript<String>("test"));
        analysisRuleMap.add(rule);

        int start = interval.current();
        int next = interval.next().current();

        for (int i = start; i <= next; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().put("test", i), "actor" + i, i);
        }

        //collector.process(analysisRuleMap.entrySet());

        JsonObject query = new JsonObject().put("tracker", projectId).mergeIn(rule.toJson()).put("frame", 5)
                .put("analysis_type", AggregationAnalysis.COUNT_X.name());
        JsonObject data = eventAnalyzer.analyze(query);

        JsonObject json = new JsonObject().put(formatTime(interval.previous().current()), ((long) (next - start)));
        for (int i = 0; i < 4; i++)
            json.put(formatTime(interval.previous().current()), 0L);

        assertEquals(data.getJsonObject("result"), json);
    }

    @Test
    public void testUniqueXAggregation_countUniqueX() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent(); 

        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, UNIQUE_X, ONE_DAY, new SimpleFieldScript<String>("test"));
        analysisRuleMap.add(rule);

        int start = interval.current();
        int next = interval.next().current();

        for (int i = start; i <= next; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().put("test", i % 100), "actor" + i, i);
        }

        //collector.process(analysisRuleMap.entrySet());

        JsonObject query = new JsonObject().put("tracker", projectId).mergeIn(rule.toJson()).put("frame", 5)
                .put("analysis_type", AggregationAnalysis.COUNT_UNIQUE_X.name());
        JsonObject data = eventAnalyzer.analyze(query);

        JsonObject json = new JsonObject().put(formatTime(interval.previous().current()), 100);
        for (int i = 0; i < 4; i++)
            json.put(formatTime(interval.previous().current()), 0);

        assertEquals(data.getJsonObject("result"), json);
    }

    @Test
    public void testUniqueXAggregation_selectUniqueX() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent(); 

        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, UNIQUE_X, ONE_DAY, new SimpleFieldScript<String>("test"));
        analysisRuleMap.add(rule);

        int start = interval.current();
        int next =  ONE_DAY.spanCurrent().next().current();

        for (int i = start; i <= next; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().put("test", "test" + (i % 100)), "actor" + i, i);
        }

        //collector.process(analysisRuleMap.entrySet());

        JsonObject query = new JsonObject().put("tracker", projectId).mergeIn(rule.toJson())
                .put("frame", 5).put("items", 100)
                .put("analysis_type", AggregationAnalysis.SELECT_UNIQUE_X.name());
        JsonObject data = eventAnalyzer.analyze(query).getJsonObject("result");

        JsonArray array = data.getJsonArray(formatTime(interval.current()));
        for (long i = 0; i < 100; i++)
            assertTrue(array.contains("test" + i));

        assertEquals(100, array.size());

        for (int i = 0; i < 4; i++)
            assertEquals(data.getJsonArray(formatTime(interval.previous().current())), new JsonArray());
    }

    @Test
    public void testUniqueXAggregation_attributeBelongsUser() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent(); 

        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, UNIQUE_X, ONE_DAY, new SimpleFieldScript<String>("_user.test"));
        analysisRuleMap.add(rule);

        for (int i = 0; i < 100; i++) {
            JsonObject map = new JsonObject();
            map.put("test", "value" + (i % 10));
//            databaseAdapter.createActor(projectId, "actor" + i, map);
        }

        int start = interval.current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            eventAggregator.aggregate(projectId, new JsonObject(), "actor" + (i % 100), i);
        }

        //collector.process(analysisRuleMap.entrySet());

        JsonObject query = new JsonObject().put("tracker", projectId).mergeIn(rule.toJson()).put("frame", 5)
                .put("analysis_type", AggregationAnalysis.SELECT_UNIQUE_X.name());
        JsonObject result = eventAnalyzer.analyze(query).getJsonObject("result");
        assertNotNull(result);

        JsonArray array = result.getJsonArray(formatTime(interval.previous().current()));
        for (long i = 0; i < 10; i++) {
            array.contains("value" + i);
        }

        for (int i = 0; i < 4; i++)
            assertEquals(result.getJsonArray(formatTime(interval.previous().current())), new JsonArray());
    }

    @Test
    public void testInterval_whenStartEndTimestamp() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent(); 

        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, COUNT, ONE_DAY);
        analysisRuleMap.add(rule);

        int start = interval.current();

        JsonObject query = new JsonObject().put("tracker", projectId).mergeIn(rule.toJson())
                .put("end", start)
                .put("start", ONE_DAY.spanCurrent().previous().previous().previous().previous().current())
                .put("analysis_type", AggregationAnalysis.COUNT.name());
        JsonObject data = eventAnalyzer.analyze( query);

        JsonObject json = new JsonObject();
        for (int i = 0; i < 5; i++) {
            json.put(formatTime(interval.current()), 0);
            interval.previous();
        }

        assertEquals(data.getJsonObject("result"), json);
    }

    @Test
    public void testInterval_whenStartEndExceeds() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent(); 

        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, COUNT, ONE_DAY);
        analysisRuleMap.add(rule);

        int start = interval.current();


        JsonObject query = new JsonObject().put("tracker", projectId).mergeIn(rule.toJson())
                .put("end", start)
                .put("start", 0)
                .put("analysis_type", AggregationAnalysis.COUNT.name());;
        JsonObject data = eventAnalyzer.analyze(query);

        assertEquals(data, new JsonObject().put("error", "there is more than 5000 items between start and times."));
    }

    @Test
    public void testInterval_whenFrameExceeds() {
        String projectId = randomString(10);

        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, COUNT, ONE_DAY);
        analysisRuleMap.add(rule);

        JsonObject query = new JsonObject().put("tracker", projectId).mergeIn(rule.toJson())
                .put("frame", 10000)
                .put("analysis_type", AggregationAnalysis.COUNT.name());
        JsonObject data = eventAnalyzer.analyze(query);

        assertEquals(data, new JsonObject().put("error", "items must be lower than 5000"));
    }
}
