package org.rakam.analysis;

import org.junit.Test;
import org.rakam.analysis.query.simple.SimpleFieldScript;
import org.rakam.analysis.rule.aggregation.TimeSeriesAggregationRule;
import org.rakam.constant.AggregationAnalysis;
import org.rakam.util.Interval;
import org.rakam.util.json.JsonArray;
import org.rakam.util.json.JsonObject;

import java.time.ZonedDateTime;
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

/**
 * Created by buremba <Burak Emre Kabakcı> on 2/10/14 02:31.
 */
public class MergingGroupingTimeSeriesTest extends AnalysisBaseTest {

    private static final Interval TWO_DAYS = Interval.parse("2day");
    private static final Interval ONE_DAY = Interval.parse("1day");

    public static void assertMerged(JsonObject actual, JsonObject merged, Interval interval, BiFunction<Long, Long, Long> function) {
        JsonObject actualResult = actual.getJsonObject("result");
        JsonObject mergedResult = merged.getJsonObject("result");
        assertTrue("query result must contain at least one frame.", actualResult.size()>0);

        Map<String, List<String>> result = actualResult.stream().map(e -> e.getKey())
                .collect(Collectors.groupingBy(x -> interval.span((int) ZonedDateTime.parse(x).toEpochSecond()).current() + "000"));
        result.forEach((key, items) -> {
            JsonObject grouped = new JsonObject();
            for (String item : items) {
                actualResult.getJsonObject(item).getMap().forEach((actualKey, actualVal) -> {
                    if (!grouped.containsKey(actualKey)) {
                        grouped.put(actualKey, (Number) actualVal);
                    } else {
                        grouped.put(actualKey, function.apply(grouped.getLong(actualKey), (long) actualVal));
                    }
                });
            }

            final StringBuilder errMessage = new StringBuilder();
            errMessage.append(String.format("merged timestamp: %s keys:", key));
            items.forEach(x -> errMessage.append(" [" + x + ":" + actualResult.getJsonObject(x) + "]"));
            errMessage.append("}");
            JsonObject m = mergedResult.getJsonObject(formatTime((int) (Long.parseLong(key)/1000)));
            assertEquals(errMessage.toString(), grouped, m);
        });

    }

    @Test
    public void testCountAggregation() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = TWO_DAYS.spanCurrent();
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, COUNT, ONE_DAY, null, null, new SimpleFieldScript<String>("key"));
        analysisRuleMap.add(rule);


        int start = interval.previous().current();
        int next = interval.next().current();
        for (int i = start; i < next; i++) {
            JsonObject m = new JsonObject().put("key", "value" + i % 3);
            eventAggregator.aggregate(projectId, m, "actor" + i, i);
        }

        //collector.process(analysisRuleMap.entrySet());

        JsonObject jsonObject = new JsonObject().put("tracker", projectId)
                .mergeIn(rule.toJson())
                .put("analysis_type", AggregationAnalysis.COUNT.name())
                .put("frame", 5);
        JsonObject d0 = eventAnalyzer.analyze(jsonObject);

        JsonObject jsonObject1 = new JsonObject().put("tracker", projectId).mergeIn(rule.toJson())
                .put("interval", TWO_DAYS.toJson())
                .put("analysis_type", AggregationAnalysis.COUNT.name())
                .put("frame", 5);
        JsonObject d1 = eventAnalyzer.analyze(jsonObject1);

        assertMerged(d0, d1, TWO_DAYS, (x,y) -> x+y);
    }

    @Test
    public void testCountXAggregation() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = TWO_DAYS.spanCurrent();
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, COUNT_X, ONE_DAY, new SimpleFieldScript<String>("key2"), null, new SimpleFieldScript<String>("key1"));
        analysisRuleMap.add(rule);

        int start = interval.previous().current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().put("key1", "value" + i % 2).put("key2", "value"), "actor" + i, i);
            eventAggregator.aggregate(projectId, new JsonObject().put("key1", "value" + i % 2), "actor" + i, i);
        }

        //collector.process(analysisRuleMap.entrySet());

        JsonObject query = new JsonObject().put("tracker", projectId)
                .mergeIn(rule.toJson())
                .put("analysis_type", AggregationAnalysis.COUNT_X.name())
                .put("frame", 5);
        JsonObject d0 = eventAnalyzer.analyze(query);

        JsonObject query1 = new JsonObject().put("tracker", projectId)
                .mergeIn(rule.toJson())
                .put("analysis_type", AggregationAnalysis.COUNT_X.name())
                .put("interval", TWO_DAYS.toJson())
                .put("frame", 5);
        JsonObject d1 = eventAnalyzer.analyze(query1);

        assertMerged(d0, d1, TWO_DAYS, (x,y) -> x+y);
    }


    @Test
    public void testSumXAggregation() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = TWO_DAYS.spanCurrent();
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, SUM_X, ONE_DAY, new SimpleFieldScript<String>("key2"), null, new SimpleFieldScript<String>("key1"));
        analysisRuleMap.add(rule);

        int start = interval.previous().current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            JsonObject m = new JsonObject().put("key1", "value" + i % 3).put("key2", 3);
            eventAggregator.aggregate(projectId, m, "actor" + i, i);
        }

        //collector.process(analysisRuleMap.entrySet());

        JsonObject jsonObject = new JsonObject().put("tracker", projectId)
                .mergeIn(rule.toJson())
                .put("analysis_type", AggregationAnalysis.SUM_X.name())
                .put("frame", 5);
        JsonObject d0 = eventAnalyzer.analyze(jsonObject);

        JsonObject query = new JsonObject().put("tracker", projectId)
                .mergeIn(rule.toJson())
                .put("analysis_type", AggregationAnalysis.SUM_X.name())
                .put("interval", TWO_DAYS.toJson())
                .put("frame", 5);
        JsonObject d1 = eventAnalyzer.analyze(query);

        assertMerged(d0, d1, TWO_DAYS, (x,y) -> x+y);
    }


    @Test
    public void testMaxXAggregation() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = TWO_DAYS.spanCurrent();
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, MAXIMUM_X, ONE_DAY, new SimpleFieldScript<String>("test"), null, new SimpleFieldScript<String>("key1"));
        analysisRuleMap.add(rule);

        int start = interval.previous().current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().put("test", i).put("key1", "value"+i%3), "actor" + i, i);
        }

        //collector.process(analysisRuleMap.entrySet());

        JsonObject jsonObject = new JsonObject().put("tracker", projectId)
                .mergeIn(rule.toJson())
                .put("analysis_type", AggregationAnalysis.MAXIMUM_X.name())
                .put("frame", 5);
        JsonObject d0 = eventAnalyzer.analyze(jsonObject);

        JsonObject jsonObject1 = new JsonObject().put("tracker", projectId).mergeIn(rule.toJson())
                .put("interval", TWO_DAYS.toJson())
                .put("analysis_type", AggregationAnalysis.MAXIMUM_X.name())
                .put("frame", 5);
        JsonObject d1 = eventAnalyzer.analyze(jsonObject1);

        assertMerged(d0, d1, TWO_DAYS, (x,y) -> Math.max(x, y));
    }


    @Test
    public void testMinXAggregation() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = TWO_DAYS.spanCurrent();
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, MINIMUM_X, ONE_DAY, new SimpleFieldScript<String>("test"), null, new SimpleFieldScript<String>("key1"));
        analysisRuleMap.add(rule);

        int start = interval.previous().current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            JsonObject m = new JsonObject().put("test", i).put("key1", "value" + i % 3);
            eventAggregator.aggregate(projectId, m, "actor" + i, i);
        }

        //collector.process(analysisRuleMap.entrySet());

        JsonObject jsonObject = new JsonObject().put("tracker", projectId)
                .mergeIn(rule.toJson())
                .put("analysis_type", AggregationAnalysis.MINIMUM_X.name())
                .put("frame", 5);
        JsonObject d0 = eventAnalyzer.analyze(jsonObject);

        JsonObject jsonObject1 = new JsonObject().put("tracker", projectId).mergeIn(rule.toJson())
                .put("interval", TWO_DAYS.toJson())
                .put("analysis_type", AggregationAnalysis.MINIMUM_X.name())
                .put("frame", 5);
        JsonObject d1 = eventAnalyzer.analyze(jsonObject1);

        assertMerged(d0, d1, TWO_DAYS, (x,y) -> Math.min(x, y));
    }


    @Test
    public void testAverageXAggregation() {
        String projectId = randomString(10);
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, AVERAGE_X, ONE_DAY, new SimpleFieldScript<String>("test"), null, new SimpleFieldScript<String>("key1"));
        analysisRuleMap.add(rule);

        int start = ONE_DAY.spanCurrent().previous().previous().current();
        int next = ONE_DAY.spanCurrent().current();

        for (int i = start; i < next; i++) {
            JsonObject m = new JsonObject().put("test", i).put("key1", "value" + i % 2);;
            eventAggregator.aggregate(projectId, m, "actor" + i, i);
        }

        //collector.process(analysisRuleMap.entrySet());

        JsonObject jsonObject = new JsonObject().put("tracker", projectId)
                .mergeIn(rule.toJson())
                .put("analysis_type", AggregationAnalysis.AVERAGE_X.name())
                .put("frame", 5);
        JsonObject d0 = eventAnalyzer.analyze(jsonObject);

        JsonObject jsonObject1 = new JsonObject().put("tracker", projectId)
                .mergeIn(rule.toJson())
                .put("interval", TWO_DAYS.toJson())
                .put("analysis_type", AggregationAnalysis.AVERAGE_X.name())
                .put("frame", 5);
        JsonObject d1 = eventAnalyzer.analyze(jsonObject1);
        // it may not work but in this specific test, the counts of each time interval are equal.
        assertMerged(d0, d1, TWO_DAYS, (x, y) -> (x + y) / 2);
    }


    @Test
    public void testAverageXAggregation_whenSumX() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = TWO_DAYS.spanCurrent();
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, AVERAGE_X, ONE_DAY, new SimpleFieldScript<String>("test"), null, new SimpleFieldScript<String>("key1"));
        analysisRuleMap.add(rule);

        int start = interval.current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            JsonObject m = new JsonObject().put("test", i).put("key1", "value" + i % 3);
            eventAggregator.aggregate(projectId, m, "actor" + i, i);
        }

        //collector.process(analysisRuleMap.entrySet());

        JsonObject jsonObject = new JsonObject().put("tracker", projectId).mergeIn(rule.toJson())
                .put("analysis_type", AggregationAnalysis.SUM_X.name())
                .put("frame", 5);
        JsonObject d0 = eventAnalyzer.analyze(jsonObject);

        JsonObject jsonObject1 = new JsonObject().put("tracker", projectId).mergeIn(rule.toJson())
                .put("interval", TWO_DAYS.toJson())
                .put("analysis_type", AggregationAnalysis.SUM_X.name())
                .put("frame", 5);
        JsonObject d1 = eventAnalyzer.analyze(jsonObject1);

        assertMerged(d0, d1, TWO_DAYS, (x,y) -> x+y);
    }

    @Test
    public void testAverageXAggregation_whenCountX() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = TWO_DAYS.spanCurrent();
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, AVERAGE_X, ONE_DAY, new SimpleFieldScript<String>("test"), null, new SimpleFieldScript<String>("key1"));
        analysisRuleMap.add(rule);

        int start = interval.previous().current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            JsonObject m = new JsonObject().put("test", i).put("key1", "value" + i % 3);
            eventAggregator.aggregate(projectId, m, "actor" + i, i);
        }

        //collector.process(analysisRuleMap.entrySet());

        JsonObject jsonObject = new JsonObject().put("tracker", projectId).mergeIn(rule.toJson())
                .put("analysis_type", AggregationAnalysis.COUNT_X.name())
                .put("frame", 5);
        JsonObject d0 = eventAnalyzer.analyze(jsonObject);

        JsonObject jsonObject1 = new JsonObject().put("tracker", projectId).mergeIn(rule.toJson())
                .put("interval", TWO_DAYS.toJson())
                .put("analysis_type", AggregationAnalysis.COUNT_X.name())
                .put("frame", 5);
        JsonObject d1 = eventAnalyzer.analyze(jsonObject1);


        assertMerged(d0, d1, TWO_DAYS, (x, y) -> x + y);
    }

    @Test
    public void testUniqueXAggregation_countUniqueX() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = TWO_DAYS.spanCurrent();
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, UNIQUE_X, ONE_DAY, new SimpleFieldScript<String>("test"), null, new SimpleFieldScript<String>("key1"));
        analysisRuleMap.add(rule);

        int start = interval.previous().current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            JsonObject m = new JsonObject().put("test", "value"+i).put("key1", "value" + i % 3);
            eventAggregator.aggregate(projectId, m, "actor" + i, i);
        }

        //collector.process(analysisRuleMap.entrySet());

        JsonObject jsonObject = new JsonObject().put("tracker", projectId)
                .mergeIn(rule.toJson())
                .put("interval", TWO_DAYS.toJson())
                .put("analysis_type", AggregationAnalysis.COUNT_UNIQUE_X.name())
                .put("frame", 5);
        JsonObject d1 = eventAnalyzer.analyze(jsonObject);

        final JsonObject result = d1.getJsonObject("result");
        final String s = formatTime(interval.previous().current());
        result.forEach(key -> {
            final JsonObject items = result.getJsonObject(key.getKey());
            if (s.equals(key)) {
                for (int i = 0; i < 3; i++) {
                    final long actual = (next - start) / 3L;
                    final double errorRate = Math.abs((actual - items.getLong("value" + i)) / ((double) actual));
                    assertTrue("the error rate is higher than 2% : " + errorRate, errorRate < .02);
                }
            } else {
                assertEquals(items, new JsonObject());
            }
        });
    }

    @Test
    public void testUniqueXAggregation_selectUniqueX() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = TWO_DAYS.spanCurrent();
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, UNIQUE_X, ONE_DAY, new SimpleFieldScript<String>("test"), null, new SimpleFieldScript<String>("key1"));
        analysisRuleMap.add(rule);

        int start = interval.previous().current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            final String s = i > (start + ((next - start) / 2)) ? "1" : "0";
            JsonObject m = new JsonObject().put("test", "value"+s+i % 3).put("key1", "value" + i % 2);
            eventAggregator.aggregate(projectId, m, "actor" + i, i);
        }

        //collector.process(analysisRuleMap.entrySet());

        JsonObject jsonObject = new JsonObject().put("tracker", projectId).mergeIn(rule.toJson())
                .put("analysis_type", AggregationAnalysis.SELECT_UNIQUE_X.name())
                .put("frame", 5);
        JsonObject actualResult = eventAnalyzer.analyze(jsonObject).getJsonObject("result");

        JsonObject jsonObject1 = new JsonObject().put("tracker", projectId).mergeIn(rule.toJson())
                .put("interval", TWO_DAYS.toJson())
                .put("analysis_type", AggregationAnalysis.SELECT_UNIQUE_X.name())
                .put("frame", 5);
        JsonObject mergedResult = eventAnalyzer.analyze(jsonObject1).getJsonObject("result");


        Map<String, List<String>> result = actualResult.stream().map(e -> e.getKey())
                .collect(Collectors.groupingBy(x -> TWO_DAYS.span((int) ZonedDateTime.parse(x).toEpochSecond()).current() + "000"));
        result.forEach((key, items) -> {
            JsonObject grouped = new JsonObject();
            for (String item : items) {
                actualResult.getJsonObject(item)
                        .forEach((actual) -> {
                            if (!grouped.containsKey(actual.getKey())) {
                                grouped.put(actual.getKey(), new JsonArray((List<Object>) actual.getValue()));
                            } else {
                                ((List) actual.getValue()).forEach(grouped.getJsonArray(actual.getKey())::add);
                            }
                        });
            }

            JsonObject object = mergedResult.getJsonObject(formatTime((int) (Long.parseLong(key)/1000)));
            assertEquals(grouped, object);
        });
    }
}