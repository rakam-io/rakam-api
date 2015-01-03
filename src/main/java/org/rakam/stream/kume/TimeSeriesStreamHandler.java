package org.rakam.stream.kume;

import org.rakam.analysis.query.FieldScript;
import org.rakam.constant.AggregationType;
import org.rakam.kume.Cluster;
import org.rakam.kume.service.crdt.set.GSetService;
import org.rakam.stream.AverageCounter;
import org.rakam.stream.SimpleCounter;
import org.rakam.stream.TimeSeriesStreamProcessor;
import org.rakam.stream.kume.service.TimeSeriesValue;
import org.rakam.util.ConversionUtil;
import org.rakam.util.Interval;
import org.rakam.util.json.JsonArray;
import org.rakam.util.json.JsonElement;
import org.rakam.util.json.JsonObject;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by buremba <Burak Emre Kabakcı> on 31/12/14 02:26.
 */
public class TimeSeriesStreamHandler {

    public static class UniqueXTimeSeriesStreamProcessor implements TimeSeriesStreamProcessor {
        final TimeSeriesValue<Set<Object>> map;
        final FieldScript<Object> field;
        final Interval interval;

        public UniqueXTimeSeriesStreamProcessor(Cluster cluster,  FieldScript<Object> field, Interval interval) {
            this.map = cluster.createService(bus -> new TimeSeriesValue<>(bus, GSetService::merge, 2, interval));
            this.field = field;
            this.interval = interval;
        }

        @Override
        public void handleEvent(JsonObject event, JsonObject actor) {
            Object extract = field.extract(event, actor);
            if (extract != null)
                map.apply(interval.spanCurrent().current(), (value) -> value.add(extract));
        }

        @Override
        public JsonElement get(int timestamp, int limit) {
            List<Object> value = map.get(timestamp, set -> set.stream().collect(Collectors.toList())).join();
            return new JsonArray(value);
        }
    }

    public static class SumXTimeSeriesStreamProcessor extends TimeSeriesCounterMapHandler {

        public SumXTimeSeriesStreamProcessor(Cluster cluster, FieldScript<Object> field, AggregationType type, Interval interval, boolean isOrdered, boolean lazySorted) {
            super(cluster, field, interval, isOrdered, lazySorted);
        }

        @Override
        public void handleEvent(JsonObject event, JsonObject actor) {
            Long extract = ConversionUtil.toLong(field.extract(event, actor));

            if (extract != null) {
                map.apply(interval.spanCurrent().current(), (value) -> value.add(extract));
            }
        }
    }

    public static class MinimumXTimeSeriesStreamProcessor extends TimeSeriesCounterMapHandler {

        public MinimumXTimeSeriesStreamProcessor(Cluster cluster, FieldScript<Object> field, AggregationType type, Interval interval, boolean isOrdered, boolean lazySorted) {
            super(cluster, field, interval, isOrdered, lazySorted);
        }

        @Override
        public void handleEvent(JsonObject event, JsonObject actor) {
            Long extract = ConversionUtil.toLong(field.extract(event, actor));

            if (extract != null) {
                map.apply(interval.spanCurrent().current(), (value) -> {
                    long value1 = value.getValue();
                    if (value1 < extract)
                        value.set(value1);
                });
            }
        }
    }

    public static class MaximumXTimeSeriesStreamProcessor extends TimeSeriesCounterMapHandler {

        public MaximumXTimeSeriesStreamProcessor(Cluster cluster, FieldScript<Object> field, AggregationType type, Interval interval, boolean isOrdered, boolean lazySorted) {
            super(cluster, field, interval, isOrdered, lazySorted);
        }

        @Override
        public void handleEvent(JsonObject event, JsonObject actor) {
            Long extract = ConversionUtil.toLong(field.extract(event, actor));

            if (extract != null) {
                map.apply(interval.spanCurrent().current(), (value) -> {
                    long value1 = value.getValue();
                    if (value1 > extract)
                        value.set(value1);
                });
            }
        }
    }

    public static class CountXTimeSeriesStreamProcessor extends TimeSeriesCounterMapHandler {
        public CountXTimeSeriesStreamProcessor(Cluster cluster, FieldScript<Object> field, AggregationType type, Interval interval, boolean isOrdered, boolean lazySorted) {
            super(cluster, field, interval, isOrdered, lazySorted);
        }

        @Override
        public void handleEvent(JsonObject event, JsonObject actor) {
            if (field.extract(event, actor) != null) {
                map.apply(interval.spanCurrent().current(), (value) -> value.increment());
            }
        }
    }

    public static class CountTimeSeriesStreamProcessor extends TimeSeriesCounterMapHandler {
        public CountTimeSeriesStreamProcessor(Cluster cluster, FieldScript<Object> field, AggregationType type, Interval interval, boolean isOrdered, boolean lazySorted) {
            super(cluster, field, interval, isOrdered, lazySorted);
        }

        @Override
        public void handleEvent(JsonObject event, JsonObject actor) {
            map.apply(interval.spanCurrent().current(), (value) -> value.increment());
        }
    }

    public static class AverageXTimeSeriesStreamProcessor implements TimeSeriesStreamProcessor {
        final TimeSeriesValue<AverageCounter> map;
        final FieldScript<Object> field;
        final Interval interval;

        public AverageXTimeSeriesStreamProcessor(Cluster cluster, FieldScript<Object> field, AggregationType type, Interval interval, boolean isOrdered, boolean lazySorted) {
            this.field = field;
            this.interval = interval;
            map = cluster.createService(bus -> new TimeSeriesValue<>(bus, AverageCounter::merge, 2, interval));
        }

        @Override
        public void handleEvent(JsonObject event, JsonObject actor) {
            Long extract = ConversionUtil.toLong(field.extract(event, actor));
            if (extract != null) {
                map.apply(interval.spanCurrent().current(), (value) -> value.increment(extract));
            }
        }

        @Override
        public JsonElement get(int timestamp, int limit) {
            return null;
        }
    }

    public abstract static class TimeSeriesCounterMapHandler implements TimeSeriesStreamProcessor {
        final TimeSeriesValue<SimpleCounter> map;
        final FieldScript<Object> field;
        final Interval interval;

        public TimeSeriesCounterMapHandler(Cluster cluster, FieldScript<Object> field, Interval interval, boolean isOrdered, boolean lazySorted) {
            this.field = field;
            this.interval = interval;
            // TODO: implement sorted by value and non-ordered maps
            if (!lazySorted)
                throw new UnsupportedOperationException();
            if (!isOrdered)
                throw new UnsupportedOperationException();
            map = cluster.createService(bus -> new TimeSeriesValue<>(bus, SimpleCounter::merge, 2, interval));
        }

        @Override
        public JsonElement get(int timestamp, int limit) {
            return JsonElement.valueOf(map.get(timestamp).join().getValue());
        }

    }
}
