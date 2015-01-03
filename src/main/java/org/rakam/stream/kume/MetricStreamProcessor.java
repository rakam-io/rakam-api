package org.rakam.stream.kume;

import org.rakam.analysis.query.FieldScript;
import org.rakam.stream.MetricStreamHandler;
import org.rakam.kume.Cluster;
import org.rakam.kume.service.crdt.counter.GCounterService;
import org.rakam.kume.service.crdt.set.GSetService;
import org.rakam.stream.kume.service.AverageGCounterService;
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
    SUNDAY(UniqueXMetricStreamHandler.class);

    private final Class<? extends MetricStreamHandler> mass;

    MetricStreamProcessor(Class<UniqueXMetricStreamHandler> uniqueXMetricRuleClass) {
        this.mass = uniqueXMetricRuleClass;
    }

    public abstract static class MetricCounterHandler implements MetricStreamHandler {
        final FieldScript<Number> field;
        final GCounterService counter;

        protected MetricCounterHandler(Cluster cluster, FieldScript<Number> field) {
            this.field = field;
            this.counter = cluster.createService(bus -> new GCounterService(bus, 2));
        }

        @Override
        public JsonElement get() {
            return JsonElement.valueOf(counter.syncAndGet().join());
        }

    }
    public static class UniqueXMetricStreamHandler implements MetricStreamHandler {
        final GSetService set;
        final FieldScript field;

        public UniqueXMetricStreamHandler(Cluster cluster, FieldScript field) {
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

    public static class SumXMetricStreamHandler extends MetricCounterHandler {


        protected SumXMetricStreamHandler(Cluster cluster, FieldScript<Number> field) {
            super(cluster, field);
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

    public static class MinimumXMetricStreamHandler extends MetricCounterHandler {


        protected MinimumXMetricStreamHandler(Cluster cluster, FieldScript<Number> field) {
            super(cluster, field);
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

    public static class MaximumXMetricStreamHandler extends MetricCounterHandler {

        protected MaximumXMetricStreamHandler(Cluster cluster, FieldScript<Number> field) {
            super(cluster, field);
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

    public static class CountXMetricStreamHandler extends MetricCounterHandler {
        protected CountXMetricStreamHandler(Cluster cluster, FieldScript<Number> field) {
            super(cluster, field);
        }

        @Override
        public void handleEvent(JsonObject event, JsonObject actor) {
            if(field.contains(event, actor))
                counter.increment();
        }
    }

    public static class CountMetricStreamHandler implements MetricStreamHandler {
        final GCounterService counter;

        public CountMetricStreamHandler(Cluster cluster) {
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

    public static class AverageXMetricStreamHandler implements MetricStreamHandler {
        final FieldScript<Number> field;
        final AverageGCounterService counter;

        protected AverageXMetricStreamHandler(Cluster cluster, FieldScript<Number> field) {
            this.counter = cluster.createService(bus -> new AverageGCounterService(bus, 2));
            this.field = field;
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

        @Override
        public JsonElement get() {
            return null;
        }
    }
}
