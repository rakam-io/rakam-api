package org.rakam.stream.kume;

import org.rakam.analysis.query.FieldScript;
import org.rakam.stream.MetricStreamHandler;
import org.rakam.kume.Cluster;
import org.rakam.kume.service.crdt.counter.GCounterService;
import org.rakam.kume.service.crdt.set.GSetService;
import org.rakam.util.json.JsonArray;
import org.rakam.util.json.JsonElement;
import org.rakam.util.json.JsonObject;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Created by buremba <Burak Emre Kabakcı> on 30/12/14 10:20.
 */
public enum MetricStreamProcessor {
    SUNDAY(UniqueXMetricRule.class);

    private final Class<? extends MetricStreamHandler> mass;

    MetricStreamProcessor(Class<UniqueXMetricRule> uniqueXMetricRuleClass) {
        this.mass = uniqueXMetricRuleClass;
    }

    public abstract static class MetricCounterRule implements MetricStreamHandler {
        final FieldScript<Number> field;
        final GCounterService counter;

        protected MetricCounterRule(FieldScript<Number> field, GCounterService counter) {
            this.field = field;
            this.counter = counter;
        }

        @Override
        public JsonElement get() {
            return JsonElement.valueOf(counter.syncAndGet().join());
        }

    }
    public static class UniqueXMetricRule implements MetricStreamHandler {
        final GSetService set;
        final FieldScript field;

        public UniqueXMetricRule(Cluster cluster, FieldScript field) {
            this.set = cluster.createService(bus -> new GSetService<>(bus, HashSet::new, 2));
            this.field = field;
        }

        @Override
        public void handleEvent(JsonObject event, JsonObject actor) {
            set.add(field.extract(event, actor));
        }

        @Override
        public JsonElement get() {
            CompletableFuture<Set> completableFuture = set.syncAndGet();
            return new JsonArray(completableFuture.join());
        }
    }

    public static class SumXMetricHandler extends MetricCounterRule {

        protected SumXMetricHandler(FieldScript<Number> field, GCounterService counter) {
            super(field, counter);
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
    }

    public static class MinimumXMetricRule extends MetricCounterRule {

        protected MinimumXMetricRule(FieldScript<Number> field, GCounterService counter) {
            super(field, counter);
        }

        @Override
        public void handleEvent(JsonObject event, JsonObject actor) {
            Number extract;
            try {
                extract = field.extract(event, actor);
            } catch (ClassCastException e) {
                return;
            }

            long extracted = extract.longValue();
            counter.process(value -> {
                if (value > extracted)
                    value = extracted;
            });
        }
    }

    public static class MaximumXMetricRule extends MetricCounterRule {
        protected MaximumXMetricRule(FieldScript<Number> field, GCounterService counter) {
            super(field, counter);
        }

        @Override
        public void handleEvent(JsonObject event, JsonObject actor) {
            Number extract;
            try {
                extract = field.extract(event, actor);
            } catch (ClassCastException e) {
                return;
            }

            long extracted = extract.longValue();
            counter.process(value -> {
                if (value < extracted)
                    value = extracted;
            });
        }
    }

    public static class CountXMetricRule extends MetricCounterRule {
        protected CountXMetricRule(FieldScript<Number> field, GCounterService counter) {
            super(field, counter);
        }

        @Override
        public void handleEvent(JsonObject event, JsonObject actor) {
            if(field.contains(event, actor))
                counter.increment();
        }
    }

    public static class CountMetricRule implements MetricStreamHandler {
        final GCounterService counter;

        public CountMetricRule(Cluster cluster) {
            this.counter = cluster.createService(bus -> new GCounterService(bus, 2));
        }

        public void handleEvent(JsonObject event, JsonObject actor) {
            counter.increment();
        }

        @Override
        public JsonElement get() {
            return JsonElement.valueOf(counter.syncAndGet().join());
        }
    }

    public static class AverageXMetricRule extends MetricCounterRule {
        public AverageXMetricRule(FieldScript<Number> field, GCounterService counter) {
            super(field, counter);
        }

        @Override
        public void handleEvent(JsonObject event, JsonObject actor) {
            Number extract;
            try {
                extract = field.extract(event, actor);
            } catch (ClassCastException e) {
                return;
            }

            if(extract != null)
                counter.add(extract.longValue());
        }
    }
}
