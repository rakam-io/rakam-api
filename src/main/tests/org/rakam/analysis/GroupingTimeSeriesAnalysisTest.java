package org.rakam.analysis;

import org.junit.Test;
import org.rakam.analysis.query.simple.SimpleFieldScript;
import org.rakam.analysis.rule.aggregation.TimeSeriesAggregationRule;
import org.rakam.stream.AverageCounter;
import org.rakam.constant.AggregationAnalysis;
import org.rakam.util.Interval;
import org.rakam.util.json.JsonArray;
import org.rakam.util.json.JsonObject;

import java.util.HashMap;
import java.util.Map;

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
 * Created by buremba <Burak Emre Kabakcı> on 28/09/14 20:48.
 */
public class GroupingTimeSeriesAnalysisTest extends AnalysisBaseTest {

    final static Interval ONE_DAY = Interval.parse("1day");

    @Test
    public void testCountAggregation() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent();
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, COUNT,  ONE_DAY, null, null, new SimpleFieldScript<String>("key"));
        analysisRuleMap.add(projectId, rule);


        int start = interval.current();
        int next = interval.next().current();
        for (int i = start; i < next; i++) {
            JsonObject m = new JsonObject().putString("key", "value" + i % 3);
            eventAggregator.aggregate(projectId, m, "actor" + i, i);
        }

        collector.process(analysisRuleMap.entrySet());

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson()).putNumber("frame", 5)
                .putString("analysis_type", AggregationAnalysis.COUNT.name());
        JsonObject data = eventAnalyzer.analyze(query);

        int val = (next-start) / 3;
        JsonObject jsonObject = new JsonObject().putNumber("value0", val).putNumber("value1", val).putNumber("value2", val);
        JsonObject json = new JsonObject().putObject(formatTime(interval.previous().current()), jsonObject);
        for (int i = 0; i < 4; i++)
            json.putObject(formatTime(interval.previous().current()), new JsonObject());

        assertEquals(json, data.getObject("result"));
    }

    @Test
    public void testCountXAggregation() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent();
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, COUNT_X, ONE_DAY, new SimpleFieldScript<String>("key2"), null, new SimpleFieldScript<String>("key1"));
        analysisRuleMap.add(projectId, rule);

        int start = interval.current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().putString("key1", "value" + i % 2).putString("key2", "value"), "actor" + i, i);
            eventAggregator.aggregate(projectId, new JsonObject().putString("key1", "value" + i % 2), "actor" + i, i);
        }

        collector.process(analysisRuleMap.entrySet());

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson()).putNumber("frame", 5)
                .putString("analysis_type", AggregationAnalysis.COUNT_X.name());
        JsonObject data = eventAnalyzer.analyze(query);

        long value = (next - start)/2;
        JsonObject jsonObject = new JsonObject().putNumber("value0", value).putNumber("value1", value);
        JsonObject json = new JsonObject().putObject(formatTime(interval.previous().current()), jsonObject);
        for (int i = 0; i < 4; i++)
            json.putObject(formatTime(interval.previous().current()), new JsonObject());

        assertEquals(json, data.getObject("result"));
    }


    @Test
    public void testSumXAggregation() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent(); 
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, SUM_X, ONE_DAY, new SimpleFieldScript<String>("key2"), null, new SimpleFieldScript<String>("key1"));
        analysisRuleMap.add(projectId, rule);

        int start = interval.current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            JsonObject m = new JsonObject().putString("key1", "value" + i % 3).putNumber("key2", 3);
            eventAggregator.aggregate(projectId, m, "actor" + i, i);
        }

        collector.process(analysisRuleMap.entrySet());

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson()).putNumber("frame", 5)
                .putString("analysis_type", AggregationAnalysis.SUM_X.name());
        JsonObject data = eventAnalyzer.analyze(query);

        long value = (next - start);
        JsonObject jsonObject = new JsonObject().putNumber("value0", value).putNumber("value1", value).putNumber("value2", value);
        JsonObject json = new JsonObject().putObject(formatTime(interval.previous().current()), jsonObject);
        for (int i = 0; i < 4; i++)
            json.putObject(formatTime(interval.previous().current()), new JsonObject());

        assertEquals(json, data.getObject("result"));
    }


    @Test
    public void testMaxXAggregation() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent(); 
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, MAXIMUM_X, ONE_DAY, new SimpleFieldScript<String>("test"),null, new SimpleFieldScript<String>("key1"));
        analysisRuleMap.add(projectId, rule);

        int start = interval.current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            eventAggregator.aggregate(projectId, new JsonObject().putNumber("test", i).putString("key1", "value"+i%3), "actor" + i, i);
        }

        collector.process(analysisRuleMap.entrySet());

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson()).putNumber("frame", 5)
                .putString("analysis_type", AggregationAnalysis.MAXIMUM_X.name());
        JsonObject data = eventAnalyzer.analyze(query);

        JsonObject jsonObject = new JsonObject()
                .putNumber("value2", next-1)
                .putNumber("value1", next-2)
                .putNumber("value0", next - 3);
        JsonObject json = new JsonObject().putObject(formatTime(interval.previous().current()), jsonObject);
        for (int i = 0; i < 4; i++)
            json.putObject(formatTime(interval.previous().current()), new JsonObject());

        assertEquals(json, data.getObject("result"));
    }


    @Test
    public void testMinXAggregation() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent(); 
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, MINIMUM_X, ONE_DAY, new SimpleFieldScript<String>("test"), null, new SimpleFieldScript<String>("key1"));
        analysisRuleMap.add(projectId, rule);

        int start = interval.current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            JsonObject m = new JsonObject().putNumber("test", i).putString("key1", "value" + i % 3);
            eventAggregator.aggregate(projectId, m, "actor" + i, i);
        }

        collector.process(analysisRuleMap.entrySet());

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson()).putNumber("frame", 5)
                .putString("analysis_type", AggregationAnalysis.MINIMUM_X.name());
        JsonObject data = eventAnalyzer.analyze(query);

        JsonObject jsonObject = new JsonObject().putNumber("value0", (long) start).putNumber("value1", start+1L).putNumber("value2", start + 2L);
        JsonObject json = new JsonObject().putObject(formatTime(interval.previous().current()), jsonObject);
        for (int i = 0; i < 4; i++)
            json.putObject(formatTime(interval.previous().current()), new JsonObject());

        assertEquals(json, data.getObject("result"));
    }


    @Test
    public void testAverageXAggregation() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent(); 
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, AVERAGE_X, ONE_DAY, new SimpleFieldScript<String>("test"), null, new SimpleFieldScript<String>("key1"));
        analysisRuleMap.add(projectId, rule);

        int start = interval.current();
        int next = interval.next().current();

        HashMap<String, AverageCounter> actual = new HashMap<>();
        for (int i = start; i < next; i++) {
            String value = "value" + i % 3;
            JsonObject m = new JsonObject().putNumber("test", i).putString("key1", value);;
            eventAggregator.aggregate(projectId, m, "actor" + i, i);
            final int finalI = i;
            actual.compute(value, (s, counter) -> {
                if(counter==null)
                    return new AverageCounter(finalI, 1);
                counter.add(finalI, 1);
                return counter;
            });
        }

        collector.process(analysisRuleMap.entrySet());

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson()).putNumber("frame", 5)
                .putString("analysis_type", AggregationAnalysis.AVERAGE_X.name());
        JsonObject data = eventAnalyzer.analyze(query);

        JsonObject jsonObject = new JsonObject();
        for (Map.Entry<String, AverageCounter> realData : actual.entrySet()) {
            jsonObject.putNumber(realData.getKey(), realData.getValue().getValue());
        }
        JsonObject json = new JsonObject().putObject(formatTime(interval.previous().current()), jsonObject);
        for (int i = 0; i < 4; i++)
            json.putObject(formatTime(interval.previous().current()), new JsonObject());

        assertEquals(json, data.getObject("result"));
    }


    @Test
    public void testAverageXAggregation_whenSumX() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent(); 
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, AVERAGE_X, ONE_DAY, new SimpleFieldScript<String>("test"), null, new SimpleFieldScript<String>("key1"));
        analysisRuleMap.add(projectId, rule);

        int start = interval.current();
        int next = interval.next().current();

        HashMap<String, Long> realData = new HashMap<>();
        for (int i = start; i < next; i++) {
            String value = "value" + i % 3;
            JsonObject m = new JsonObject().putNumber("test", i).putString("key1", value);
            eventAggregator.aggregate(projectId, m, "actor" + i, i);
            final long finalI = i;
            realData.compute(value, (s, counter) -> {
                if (counter == null)
                    return finalI;
                return counter+finalI;
            });
        }

        collector.process(analysisRuleMap.entrySet());

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson()).putNumber("frame", 5)
                .putString("analysis_type", AggregationAnalysis.SUM_X.name());
        JsonObject data = eventAnalyzer.analyze(query);

        JsonObject jsonObject = new JsonObject();
        for (Map.Entry<String, Long> entry : realData.entrySet()) {
            jsonObject.putNumber(entry.getKey(), entry.getValue());
        }
        JsonObject json = new JsonObject().putObject(formatTime(interval.previous().current()), jsonObject);
        for (int i = 0; i < 4; i++)
            json.putObject(formatTime(interval.previous().current()), new JsonObject());

        assertEquals(json, data.getObject("result"));
    }

    @Test
    public void testAverageXAggregation_whenCountX() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent(); 
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, AVERAGE_X, ONE_DAY, new SimpleFieldScript<String>("test"), null, new SimpleFieldScript<String>("key1"));
        analysisRuleMap.add(projectId, rule);

        int start = interval.current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            JsonObject m = new JsonObject().putNumber("test", i).putString("key1", "value" + i % 3);
            eventAggregator.aggregate(projectId, m, "actor" + i, i);
        }

        collector.process(analysisRuleMap.entrySet());

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson()).putNumber("frame", 5)
                .putString("analysis_type", AggregationAnalysis.COUNT_X.name());
        JsonObject data = eventAnalyzer.analyze(query);

        long count = (next-start)/3;
        JsonObject jsonObject = new JsonObject().putNumber("value0", count).putNumber("value1", count).putNumber("value2", count);
        JsonObject json = new JsonObject().putObject(formatTime(interval.previous().current()), jsonObject);
        for (int i = 0; i < 4; i++)
            json.putObject(formatTime(interval.previous().current()), new JsonObject());

        assertEquals(json, data.getObject("result"));
    }

    @Test
    public void testUniqueXAggregation_countUniqueX() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent(); 
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, UNIQUE_X, ONE_DAY, new SimpleFieldScript<String>("test"), null, new SimpleFieldScript<String>("key1"));
        analysisRuleMap.add(projectId, rule);

        int start = interval.current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            JsonObject m = new JsonObject().putString("test", "value"+i).putString("key1", "value" + i % 3);
            eventAggregator.aggregate(projectId, m, "actor" + i, i);
        }

        collector.process(analysisRuleMap.entrySet());

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson()).putNumber("frame", 5)
                .putString("analysis_type", AggregationAnalysis.COUNT_UNIQUE_X.name());
        JsonObject data = eventAnalyzer.analyze(query);

        JsonObject jsonObject = new JsonObject();
        long count = (next-start)/3;
        for (int i = 0; i < 3; i++) {
            jsonObject.putNumber("value"+i, count);
        }
        JsonObject json = new JsonObject().putObject(formatTime(interval.previous().current()), jsonObject);
        for (int i = 0; i < 4; i++)
            json.putObject(formatTime(interval.previous().current()), new JsonObject());

        assertEquals(json, data.getObject("result"));
    }

    @Test
    public void testUniqueXAggregation_selectUniqueX() {
        String projectId = randomString(10);
        Interval.StatefulSpanTime interval = ONE_DAY.spanCurrent(); 
        TimeSeriesAggregationRule rule = new TimeSeriesAggregationRule(projectId, UNIQUE_X, ONE_DAY, new SimpleFieldScript<String>("test"), null, new SimpleFieldScript<String>("key1"));
        analysisRuleMap.add(projectId, rule);

        int start = interval.current();
        int next = interval.next().current();

        for (int i = start; i < next; i++) {
            JsonObject m = new JsonObject().putString("test", "value"+i % 3).putString("key1", "value" + i % 2);
            eventAggregator.aggregate(projectId, m, "actor" + i, i);
        }

        collector.process(analysisRuleMap.entrySet());

        JsonObject query = new JsonObject().putString("tracker", projectId).mergeIn(rule.toJson()).putNumber("frame", 5)
                .putString("analysis_type", AggregationAnalysis.SELECT_UNIQUE_X.name());
        JsonObject data = eventAnalyzer.analyze(query);
        JsonObject result = data.getObject("result");
        assertNotNull(result);

        JsonObject json = result.getObject(formatTime(interval.previous().current()));

        for (int i = 0; i < 4; i++)
            assertEquals(result.getObject(formatTime(interval.previous().current())), new JsonObject());

        for (long i = 0; i < 2; i++) {
            JsonArray array = json.getArray("value" + i);
            for (int i1 = 0; i1 < 3; i1++) {
                assertTrue(array.contains("value" + i1));
            }
            assertEquals(array.size(), 3);
        }
    }
}
