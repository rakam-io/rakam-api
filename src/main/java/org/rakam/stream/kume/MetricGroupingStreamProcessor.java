package org.rakam.stream.kume;

import org.rakam.analysis.query.FieldScript;
import org.rakam.stream.MetricGroupingStreamHandler;
import org.rakam.stream.MetricStreamHandler;
import org.rakam.kume.Cluster;
import org.rakam.kume.service.crdt.set.GSetService;
import org.rakam.stream.SimpleCounter;
import org.rakam.stream.kume.service.AverageGCounterService;
import org.rakam.stream.kume.service.MapService;
import org.rakam.util.json.JsonArray;
import org.rakam.util.json.JsonElement;
import org.rakam.util.json.JsonObject;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by buremba <Burak Emre Kabakcı> on 30/12/14 22:07.
 */
public enum MetricGroupingStreamProcessor {
    SUNDAY(UniqueXMetricRule.class);

    private final Class<? extends MetricStreamHandler> mass;

    MetricGroupingStreamProcessor(Class<UniqueXMetricRule> uniqueXMetricRuleClass) {
        this.mass = uniqueXMetricRuleClass;
    }

    public abstract static class MetricCounterMapNumberRule extends MetricCounterMapRule {
        public MetricCounterMapNumberRule(Cluster cluster, FieldScript<String> groupBy, FieldScript<Object> field, boolean isOrdered, boolean lazySorted) {
            super(cluster, groupBy, field, isOrdered, lazySorted);
        }

        @Override
        public void handleEvent(JsonObject event, JsonObject actor) {
            Number extract;
            try {
                extract = (Number) field.extract(event, actor);
            } catch (ClassCastException e) {
                return;
            }

            String groupByValue = groupBy.extract(event, actor);
            if (extract != null && groupByValue != null)
                handleEvent(groupByValue, extract.longValue());
        }

        public abstract void handleEvent(String groupByValue, long extractedValue);


    }

    public abstract static class MetricCounterMapRule implements MetricGroupingStreamHandler {
        final FieldScript<Object> field;
        final FieldScript<String> groupBy;
        final MapService<String, SimpleCounter> map;

        public MetricCounterMapRule(Cluster cluster, FieldScript<String> groupBy, FieldScript<Object> field, boolean isOrdered, boolean lazySorted) {
            this.field = field;
            this.groupBy = groupBy;
            // TODO: implement sorted by value and non-ordered maps
            if (!lazySorted)
                throw new UnsupportedOperationException();
            if (!isOrdered)
                throw new UnsupportedOperationException();
            this.map = cluster.createService(bus -> new MapService<>(bus, ConcurrentHashMap::new, SimpleCounter::merge, 2));
        }

        @Override
        public JsonElement get(int limit) {
            return new JsonArray(map.sortAndGet(limit).join());
        }

    }

    public static class UniqueXMetricRule implements MetricStreamHandler {
        final FieldScript field;
        final FieldScript<String> groupBy;
        final MapService<String, Set> map;

        public UniqueXMetricRule(Cluster cluster, FieldScript<String> groupBy, FieldScript field) {
            this.groupBy = groupBy;
            this.field = field;
            this.map = cluster.createService(bus -> new MapService<>(bus, ConcurrentHashMap::new, GSetService::merge, 2));
        }

        @Override
        public void handleEvent(JsonObject event, JsonObject actor) {
            Object extract = field.extract(event, actor);
            if (extract != null)
                map.apply(groupBy.extract(event, actor), (value) -> value.add(extract));
        }

        @Override
        public JsonElement get() {
            Map<String, Set> value = map.syncAndGet().join();
            JsonObject entries = new JsonObject();
            value.forEach((k, v) -> entries.put(k, new JsonArray(v)));

            return entries;
        }
    }

    public static class SumXMetricHandler extends MetricCounterMapNumberRule {


        public SumXMetricHandler(Cluster cluster, FieldScript<String> groupBy, FieldScript<Object> field, boolean isOrdered, boolean lazySorted) {
            super(cluster, groupBy, field, isOrdered, lazySorted);
        }

        @Override
        public void handleEvent(String groupByValue, long extractedValue) {
            map.apply(groupByValue, (value) -> value.add(extractedValue));
        }
    }

    public static class MinimumXMetricRule extends MetricCounterMapNumberRule {


        public MinimumXMetricRule(Cluster cluster, FieldScript<String> groupBy, FieldScript<Object> field, boolean isOrdered, boolean lazySorted) {
            super(cluster, groupBy, field, isOrdered, lazySorted);
        }

        @Override
        public void handleEvent(String groupByValue, long extractedValue) {
            map.apply(groupByValue, (value) -> {
                long value1 = value.getValue();
                if(value1 < extractedValue)
                    value.set(value1);
            });
        }

    }

    public static class MaximumXMetricRule extends MetricCounterMapNumberRule {

        public MaximumXMetricRule(Cluster cluster, FieldScript<String> groupBy, FieldScript<Object> field, boolean isOrdered, boolean lazySorted) {
            super(cluster, groupBy, field, isOrdered, lazySorted);
        }

        @Override
        public void handleEvent(String groupByValue, long extractedValue) {
            map.apply(groupByValue, (value) -> {
                long value1 = value.getValue();
                if(value1 > extractedValue)
                    value.set(value1);
            });
        }
    }

    public static class CountXMetricRule extends MetricCounterMapRule {
        public CountXMetricRule(Cluster cluster, FieldScript<String> groupBy, FieldScript<Object> field, boolean isOrdered, boolean lazySorted) {
            super(cluster, groupBy, field, isOrdered, lazySorted);
        }

        @Override
        public void handleEvent(JsonObject event, JsonObject actor) {
            String groupByValue = groupBy.extract(event, actor);
            Object extracted = field.extract(event, actor);
            if(extracted!=null && groupByValue!=null)
                map.apply(groupByValue, (value) -> value.increment());
        }
    }

    public static class CountMetricRule extends MetricCounterMapRule {
        public CountMetricRule(Cluster cluster, FieldScript<String> groupBy, FieldScript<Object> field, boolean isOrdered, boolean lazySorted) {
            super(cluster, groupBy, field, isOrdered, lazySorted);
        }

        @Override
        public void handleEvent(JsonObject event, JsonObject actor) {
            String groupByValue = groupBy.extract(event, actor);
            if(groupByValue!=null)
                map.apply(groupByValue, (value) -> value.increment());
        }
    }

    public static class AverageXMetricRule implements MetricGroupingStreamHandler {
        final FieldScript<Number> field;
        final AverageGCounterService counter;

        public AverageXMetricRule(Cluster cluster, FieldScript<Number> field) {
            this.field = field;
            this.counter = cluster.createService(bus -> new AverageGCounterService(bus, 2));
        }

        @Override
        public void handleEvent(JsonObject event, JsonObject actor) {
            Number extract;
            try {
                extract = field.extract(event, actor);
            } catch (ClassCastException e) {
                return;
            }

            counter.add(extract.longValue());
        }

        @Override
        public JsonElement get(int limit) {
            return null;
        }

    }

}