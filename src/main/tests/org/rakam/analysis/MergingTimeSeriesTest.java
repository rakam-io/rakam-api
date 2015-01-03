package org.rakam.analysis;

import org.rakam.util.Interval;

/**
 * Created by buremba <Burak Emre Kabakcı> on 19/09/14 13:57.
 */
public class MergingTimeSeriesTest extends AnalysisBaseTest {
    private static final Interval TWO_DAYS = Interval.parse("2day");
    private static final Interval ONE_DAY = Interval.parse("1day");

//    @Test
//    public void testCountAggregation() {
//        String projectId = randomString(10);
//        Interval.StatefulSpanTime interval = TWO_DAYS.spanCurrent();
//
//        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, COUNT, ONE_DAY);
//        analysisRuleMap.add(projectId, rule);
//
//        int start = interval.current();
//        int next = interval.next().current();
//        for (int i = start; i < next; i++) {
//            JsonObject m = iterativeJson(3, "key", "value");
//            eventAggregator.aggregate(projectId, m, "actor" + i, i);
//        }
//
//        collector.process(analysisRuleMap.entrySet());
//
//        JsonObject query = new JsonObject().put("tracker", projectId).mergeIn(rule.toJson())
//                .putValue("interval", TWO_DAYS.toJson())
//                .put("frame", 2).put("analysis_type", AggregationAnalysis.COUNT.name());
//        JsonObject data = eventAnalyzer.analyze(query);
//
//        JsonObject json = new JsonObject().put(formatTime(interval.previous().current()), ((long) (next - start)));
//        json.put(formatTime(interval.previous().current()), 0L);
//
//        assertEquals(data.getJsonObject("result"), json);
//    }
//
//    @Test
//    public void testCountXAggregation() {
//        String projectId = randomString(10);
//        Interval.StatefulSpanTime interval = TWO_DAYS.spanCurrent();
//
//        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, COUNT_X, ONE_DAY, new SimpleFieldScript<String>("key2"));
//        analysisRuleMap.add(projectId, rule);
//
//        int start = interval.current();
//        int next = interval.next().current();
//
//        for (int i = start; i < next; i++) {
//            eventAggregator.aggregate(projectId, iterativeJson(2, "key", "value"), "actor" + i, i);
//            eventAggregator.aggregate(projectId, iterativeJson(3, "key", "value"), "actor" + i, i);
//        }
//
//        collector.process(analysisRuleMap.entrySet());
//
//        JsonObject query = new JsonObject().put("tracker", projectId).mergeIn(rule.toJson())
//                .putValue("interval", TWO_DAYS.toJson())
//                .put("frame", 5).put("analysis_type", AggregationAnalysis.COUNT_X.name());
//        JsonObject data = eventAnalyzer.analyze(query);
//
//        JsonObject json = new JsonObject().put(formatTime(interval.previous().current()), ((long) (next - start)));
//        for (int i = 0; i < 4; i++)
//            json.put(formatTime(interval.previous().current()), 0L);
//
//        assertEquals(data.getJsonObject("result"), json);
//    }
//
//
//    @Test
//    public void testSumXAggregation() {
//        String projectId = randomString(10);
//        Interval.StatefulSpanTime interval = TWO_DAYS.spanCurrent();
//
//        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, SUM_X, ONE_DAY, new SimpleFieldScript<String>("test"));
//        analysisRuleMap.add(projectId, rule);
//
//        int start = interval.current();
//        int next = interval.next().current();
//
//        long sum = 0;
//        for (int i = start; i < next; i++) {
//            eventAggregator.aggregate(projectId, new JsonObject().put("test", i), "actor" + i, i);
//            sum += i;
//        }
//
//        collector.process(analysisRuleMap.entrySet());
//
//        JsonObject query = new JsonObject().put("tracker", projectId).mergeIn(rule.toJson())
//                .putValue("interval", TWO_DAYS.toJson())
//                .put("frame", 5).put("analysis_type", AggregationAnalysis.SUM_X.name());
//        JsonObject data = eventAnalyzer.analyze(query);
//
//
//        JsonObject json = new JsonObject().put(formatTime(interval.previous().current()), sum);
//        for (int i = 0; i < 4; i++)
//            json.put(formatTime(interval.previous().current()), 0L);
//
//        assertEquals(data.getJsonObject("result"), json);
//    }
//
//
//    @Test
//    public void testMaxXAggregation() {
//        String projectId = randomString(10);
//        Interval.StatefulSpanTime interval = TWO_DAYS.spanCurrent();
//
//        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, MAXIMUM_X, ONE_DAY, new SimpleFieldScript<String>("test"));
//        analysisRuleMap.add(projectId, rule);
//
//        int start = interval.current();
//        int next = interval.next().current();
//
//        for (int i = start; i < next; i++) {
//            eventAggregator.aggregate(projectId, new JsonObject().put("test", i), "actor" + i, i);
//        }
//
//        collector.process(analysisRuleMap.entrySet());
//
//        JsonObject query = new JsonObject().put("tracker", projectId).mergeIn(rule.toJson())
//                .putValue("interval", TWO_DAYS.toJson())
//                .put("frame", 5).put("analysis_type", AggregationAnalysis.MAXIMUM_X.name());
//        JsonObject data = eventAnalyzer.analyze(query);
//
//
//        JsonObject json = new JsonObject().put(formatTime(interval.previous().current()), next - 1L);
//        for (int i = 0; i < 4; i++)
//            json.put(formatTime(interval.previous().current()), null);
//
//        assertEquals(data.getJsonObject("result"), json);
//    }
//
//
//    @Test
//    public void testMinXAggregation() {
//        String projectId = randomString(10);
//        Interval.StatefulSpanTime interval = TWO_DAYS.spanCurrent();
//
//        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, MINIMUM_X, ONE_DAY, new SimpleFieldScript<String>("test"));
//        analysisRuleMap.add(projectId, rule);
//
//        int start = interval.previous().current();
//        int next = interval.next().current();
//
//        for (int i = start; i < next; i++) {
//            eventAggregator.aggregate(projectId, new JsonObject().put("test", i), "actor" + i, i);
//        }
//
//        collector.process(analysisRuleMap.entrySet());
//
//        JsonObject query = new JsonObject().put("tracker", projectId).mergeIn(rule.toJson())
//                .putValue("interval", TWO_DAYS.toJson())
//                .put("frame", 5)
//                .put("analysis_type", AggregationAnalysis.MINIMUM_X.name());
//        JsonObject data = eventAnalyzer.analyze(query);
//
//        JsonObject json = new JsonObject().put(formatTime(interval.previous().current()), ((long) start));
//        for (int i = 0; i < 4; i++)
//            json.putValue(formatTime(interval.previous().current()), null);
//
//        assertEquals(json, data.getJsonObject("result"));
//    }
//
//
//    @Test
//    public void testAverageXAggregation() {
//        String projectId = randomString(10);
//        Interval.StatefulSpanTime interval = TWO_DAYS.spanCurrent();
//
//        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, AVERAGE_X, ONE_DAY, new SimpleFieldScript<String>("test"));
//        analysisRuleMap.add(projectId, rule);
//
//        int start = interval.current();
//        int next = interval.next().current();
//
//        long sum = 0;
//        for (int i = start; i < next; i++) {
//            sum += i;
//            eventAggregator.aggregate(projectId, new JsonObject().put("test", i), "actor" + i, i);
//        }
//
//        collector.process(analysisRuleMap.entrySet());
//
//        JsonObject query = new JsonObject().put("tracker", projectId).mergeIn(rule.toJson())
//                .putValue("interval", TWO_DAYS.toJson())
//                .put("frame", 5)
//                .put("analysis_type", AggregationAnalysis.AVERAGE_X.name());
//        JsonObject data = eventAnalyzer.analyze(query);
//
//
//        JsonObject json = new JsonObject().put(formatTime(interval.previous().current()), sum / (next - start));
//        for (int i = 0; i < 4; i++)
//            json.put(formatTime(interval.previous().current()), 0L);
//
//        assertEquals(data.getJsonObject("result"), json);
//    }
//
//
//    @Test
//    public void testAverageXAggregation_whenSumX() {
//        String projectId = randomString(10);
//        Interval.StatefulSpanTime interval = TWO_DAYS.spanCurrent();
//
//        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, AVERAGE_X, ONE_DAY, new SimpleFieldScript<String>("test"));
//        analysisRuleMap.add(projectId, rule);
//
//        int start = interval.current();
//        int next = interval.next().current();
//
//        long sum = 0;
//        for (int i = start; i < next; i++) {
//            sum += i;
//            eventAggregator.aggregate(projectId, new JsonObject().put("test", i), "actor" + i, i);
//        }
//
//        collector.process(analysisRuleMap.entrySet());
//
//        JsonObject query = new JsonObject().put("tracker", projectId).mergeIn(rule.toJson())
//                .putValue("interval", TWO_DAYS.toJson())
//                .put("frame", 5)
//                .put("analysis_type", AggregationAnalysis.SUM_X.name());
//        JsonObject data = eventAnalyzer.analyze(query);
//
//        JsonObject json = new JsonObject().put(formatTime(interval.previous().current()), sum);
//        for (int i = 0; i < 4; i++)
//            json.put(formatTime(interval.previous().current()), 0L);
//
//        assertEquals(data.getJsonObject("result"), json);
//    }
//
//    @Test
//    public void testAverageXAggregation_whenCountX() {
//        String projectId = randomString(10);
//        Interval.StatefulSpanTime interval = TWO_DAYS.spanCurrent();
//
//        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, AVERAGE_X, ONE_DAY, new SimpleFieldScript<String>("test"));
//        analysisRuleMap.add(projectId, rule);
//
//        int start = interval.current();
//        int next = interval.next().current();
//
//        for (int i = start; i < next; i++) {
//            eventAggregator.aggregate(projectId, new JsonObject().put("test", i), "actor" + i, i);
//        }
//
//        collector.process(analysisRuleMap.entrySet());
//
//        JsonObject query = new JsonObject().put("tracker", projectId).mergeIn(rule.toJson())
//                .putValue("interval", TWO_DAYS.toJson())
//                .put("frame", 5)
//                .put("analysis_type", AggregationAnalysis.COUNT_X.name());
//        JsonObject data = eventAnalyzer.analyze(query);
//
//
//        JsonObject json = new JsonObject().put(formatTime(interval.previous().current()), ((long) (next - start)));
//        for (int i = 0; i < 4; i++)
//            json.put(formatTime(interval.previous().current()), 0L);
//
//        assertEquals(data.getJsonObject("result"), json);
//    }
//
//    @Test
//    public void testUniqueXAggregation_countUniqueX() {
//        String projectId = randomString(10);
//        Interval.StatefulSpanTime interval = TWO_DAYS.spanCurrent();
//
//        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, UNIQUE_X, ONE_DAY, new SimpleFieldScript<String>("test"));
//        analysisRuleMap.add(projectId, rule);
//
//        int start = interval.current();
//        int next = interval.next().current();
//
//        for (int i = start; i < next; i++) {
//            eventAggregator.aggregate(projectId, new JsonObject().put("test", i % 100), "actor" + i, i);
//        }
//
//        collector.process(analysisRuleMap.entrySet());
//
//        JsonObject query = new JsonObject().put("tracker", projectId).mergeIn(rule.toJson())
//                .putValue("interval", TWO_DAYS.toJson())
//                .put("frame", 5)
//                .put("analysis_type", AggregationAnalysis.COUNT_UNIQUE_X.name());
//
//        JsonObject data = eventAnalyzer.analyze(query);
//
//        JsonObject json = new JsonObject().put(formatTime(interval.previous().current()), 100);
//        for (int i = 0; i < 4; i++)
//            json.put(formatTime(interval.previous().current()), 0);
//
//        assertEquals(data.getJsonObject("result"), json);
//    }
//
//    @Test
//    public void testUniqueXAggregation_selectUniqueX() {
//        String projectId = randomString(10);
//        Interval.StatefulSpanTime interval = TWO_DAYS.spanCurrent();
//
//        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, UNIQUE_X, ONE_DAY, new SimpleFieldScript<String>("test"));
//        analysisRuleMap.add(projectId, rule);
//
//        int start = interval.current();
//        int next = interval.next().current();
//
//        for (int i = start; i < next; i++) {
//            eventAggregator.aggregate(projectId, new JsonObject().put("test", "test" + (i % 100)), "actor" + i, i);
//        }
//
//        collector.process(analysisRuleMap.entrySet());
//
//        JsonObject query = new JsonObject().put("tracker", projectId).mergeIn(rule.toJson())
//                .putValue("interval", TWO_DAYS.toJson())
//                .put("frame", 5)
//                .put("items", 100)
//                .put("analysis_type", AggregationAnalysis.SELECT_UNIQUE_X.name());
//        JsonObject data = eventAnalyzer.analyze(query).getJsonObject("result");
//
//        JsonArray array = data.getArray(formatTime(interval.previous().current()));
//        assertNotNull(array);
//        for (long i = 0; i < 100; i++)
//            assertTrue(array.contains("test" + i));
//        assertEquals(100, array.size());
//
//        for (int i = 0; i < 4; i++)
//            assertEquals(data.getArray(formatTime(interval.previous().current())), new JsonArray());
//    }
}
