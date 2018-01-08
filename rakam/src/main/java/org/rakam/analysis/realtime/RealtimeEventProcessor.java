package org.rakam.analysis.realtime;

import com.google.common.primitives.Ints;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.cookie.Cookie;
import org.apache.avro.generic.GenericRecord;
import org.rakam.analysis.realtime.RealtimeService.RealTimeQueryResult;
import org.rakam.collection.Event;
import org.rakam.plugin.EventMapper;
import org.rakam.report.realtime.AggregationType;
import org.rakam.report.realtime.RealTimeConfig;
import org.rakam.report.realtime.RealTimeReport;
import org.rakam.util.RakamException;
import org.weakref.jmx.internal.guava.cache.CacheBuilder;
import org.weakref.jmx.internal.guava.cache.CacheLoader;
import org.weakref.jmx.internal.guava.cache.LoadingCache;
import org.weakref.jmx.internal.guava.collect.ImmutableList;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.net.InetAddress;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_IMPLEMENTED;
import static java.util.concurrent.TimeUnit.*;
import static org.rakam.report.realtime.AggregationType.APPROXIMATE_UNIQUE;
import static org.rakam.report.realtime.AggregationType.COUNT_UNIQUE;
import static org.rakam.util.ValidationUtil.checkNotNull;

@Singleton
public class RealtimeEventProcessor
        implements EventMapper {
    private final Map<RealtimeTable, Map<Integer, Map<List<Object>, List<AbstractMetric>>>> tables;
    private final RealTimeConfig config;
    private final LoadingCache<String, List<RealTimeReport>> reports;
    private final RealtimeMetadataService metadata;
    private final long sliceIntervalInMillis;
    private final int sliceIntervalInSeconds;
    private ScheduledExecutorService scheduledExecutor;
    private ExecutorService executorService;

    @Inject
    public RealtimeEventProcessor(RealTimeConfig config, RealtimeMetadataService metadata) {
        this.tables = new ConcurrentHashMap<>();
        this.config = config;
        this.metadata = metadata;
        sliceIntervalInMillis = config.getSlideInterval().toMillis();
        sliceIntervalInSeconds = (int) config.getSlideInterval().getValue(SECONDS);
        reports = CacheBuilder.newBuilder()
                .expireAfterAccess(15, MINUTES)
                .build(new CacheLoader<String, List<RealTimeReport>>() {
                    @Override
                    public List<RealTimeReport> load(String project)
                            throws Exception {
                        return metadata.list(project);
                    }
                });
    }

    @PostConstruct
    public void start() {
        scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        executorService = Executors.newSingleThreadExecutor();
        scheduledExecutor.scheduleWithFixedDelay(this::expire,
                config.getSlideInterval().toMillis(),
                config.getSlideInterval().toMillis(), MILLISECONDS);
    }

    private void expire() {
        long expiredEpoch = (Instant.now().toEpochMilli() - config.getWindowInterval().toMillis()) / config.getSlideInterval().toMillis();
        for (Map.Entry<RealtimeTable, Map<Integer, Map<List<Object>, List<AbstractMetric>>>> entry : tables.entrySet()) {
            Iterator<Map.Entry<Integer, Map<List<Object>, List<AbstractMetric>>>> iterator = entry.getValue().entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Integer, Map<List<Object>, List<AbstractMetric>>> next = iterator.next();
                if (next.getKey() <= expiredEpoch) {
//                    executorService.execute(iterator::remove);
                }
            }
        }
    }

    public CompletableFuture<RealTimeQueryResult> query(String project,
                                                        String tableName,
                                                        String filter,
                                                        RealTimeReport.Measure measure,
                                                        List<String> dimensions,
                                                        Boolean aggregate,
                                                        Instant dateStart,
                                                        Instant dateEnd) {
        if (filter != null) {
            throw new RakamException("Filter in real-table query is not supported.", NOT_IMPLEMENTED);
        }

        Map<Integer, Map<List<Object>, List<AbstractMetric>>> map = tables.get(new RealtimeTable(project, tableName));
        Object result;
        if (map == null || map.isEmpty()) {
            result = aggregate && dimensions.isEmpty() ? null : ImmutableList.of();
        } else {
            RealTimeReport realTimeReport = metadata.get(project, tableName);
            List<String> dimensionList = realTimeReport.dimensions;
            int measureIndex = realTimeReport.measures.indexOf(measure);

            int[] collect = Optional.ofNullable(dimensions).orElse(ImmutableList.of())
                    .stream()
                    .mapToInt(e -> {
                        int i = dimensionList.indexOf(e);
                        if (i < 0) {
                            throw new RakamException("Dimension doesn't exist", BAD_REQUEST);
                        }
                        return i;
                    })
                    .toArray();

            HashMap<List<Object>, AbstractMetric> dimensionMap = new HashMap<>();
            int columnSize = (aggregate ? 0 : 1) + (dimensions != null ? dimensions.size() : 0);

            Integer end = Ints.checkedCast(dateEnd.toEpochMilli() / sliceIntervalInMillis);
            Integer cursor = Ints.checkedCast(dateStart.toEpochMilli() / sliceIntervalInMillis);
            while (cursor++ < end) {
                Map<List<Object>, List<AbstractMetric>> values = map.get(cursor);

                if (values == null) {
                    continue;
                }

                ArrayList<Object> objects = new ArrayList<>(columnSize);
                if (!aggregate) {
                    objects.add(cursor * sliceIntervalInSeconds);
                }

                for (Map.Entry<List<Object>, List<AbstractMetric>> entry : values.entrySet()) {
                    List<Object> key = entry.getKey();
                    for (int integer : collect) {
                        Object e = key.get(integer);
                        objects.add(e == null ? "(not set)" : e);
                    }

                    AbstractMetric metric = entry.getValue().get(measureIndex);

                    AbstractMetric o = dimensionMap.get(objects);
                    if (o == null) {
                        o = metric;
                        dimensionMap.put(objects, o);
                    } else {
                        o.merge(metric);
                    }
                }
            }

            List<List<Object>> value = new ArrayList<>(dimensionMap.size());
            for (Map.Entry<List<Object>, AbstractMetric> entry : dimensionMap.entrySet()) {
                ArrayList<Object> objects = new ArrayList<>(columnSize);
                objects.addAll(entry.getKey());
                objects.add(entry.getValue().value());
                value.add(objects);
            }

            result = value;
        }

        return CompletableFuture.completedFuture(new RealTimeQueryResult(
                dateStart.getEpochSecond(), dateEnd.getEpochSecond(), sliceIntervalInSeconds, result));
    }

    @Override
    public CompletableFuture<List<Cookie>> mapAsync(Event event, RequestParams requestParams, InetAddress sourceAddress, HttpHeaders responseHeaders) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                List<RealTimeReport> realTimeReports = reports.getUnchecked(event.project());
                if (realTimeReports == null) {
                    return null;
                }

                Predicate<GenericRecord> filter = (record) -> true;

                for (RealTimeReport report : realTimeReports) {
                    RealtimeTable key = new RealtimeTable(event.project(), report.table_name);
                    Map<Integer, Map<List<Object>, List<AbstractMetric>>> table = tables.get(key);

                    if (report.collections.isEmpty() && !report.collections.contains(event.collection())) {
                        continue;
                    }

                    if (!filter.test(event.properties())) {
                        continue;
                    }

                    if (table == null) {
                        table = new ConcurrentHashMap<>();
                        tables.put(key, table);
                    }

                    Integer now = Ints.checkedCast(System.currentTimeMillis() / config.getSlideInterval().toMillis());
                    Map<List<Object>, List<AbstractMetric>> bucket = table.get(now);

                    if (bucket == null) {
                        bucket = new ConcurrentHashMap<>();
                        table.put(now, bucket);
                    }

                    List<Object> dimensions = report.dimensions.stream()
                            .map(s -> event.properties().get(s))
                            .collect(Collectors.toList());

                    List<AbstractMetric> metrics = bucket.get(dimensions);
                    if (metrics == null) {
                        metrics = report.measures.stream()
                                .map(measure -> createMetric(measure))
                                .collect(Collectors.toList());
                        bucket.put(dimensions, metrics);
                    }

                    for (AbstractMetric metric : metrics) {
                        metric.apply(event.properties());
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }, executorService);
    }

    private AbstractMetric createMetric(RealTimeReport.Measure e) {
        if (e.aggregation == AggregationType.SUM) {
            return new SumMetric(e.column);
        }

        if (e.aggregation == AggregationType.COUNT) {
            return new CountMetric(e.column);
        }

        if (e.aggregation == AggregationType.AVERAGE) {
            return new AverageMetric(e.column);
        }

        if (e.aggregation == AggregationType.MAXIMUM) {
            return new MaximumMinimumMetric(e.column, true);
        }

        if (e.aggregation == AggregationType.MINIMUM) {
            return new MaximumMinimumMetric(e.column, false);
        }

        if (e.aggregation == APPROXIMATE_UNIQUE || e.aggregation == COUNT_UNIQUE) {
            return new UniqueCountMetric(e.column);
        }

        throw new IllegalStateException("Aggregation method is not supported");
    }

    public static abstract class AbstractMetric<T> {
        protected final String fieldName;

        public AbstractMetric(String fieldName) {
            this.fieldName = fieldName;
        }

        public abstract Number value();

        public abstract T copy();

        public abstract void apply(GenericRecord record);

        public abstract void merge(T metric);
    }

    public static class SumMetric
            extends AbstractMetric<SumMetric> {

        private double value;

        public SumMetric(String fieldName) {
            super(fieldName);
        }

        @Override
        public Number value() {
            return value;
        }

        @Override
        public SumMetric copy() {
            SumMetric sumMetric = new SumMetric(fieldName);
            sumMetric.value = value;
            return sumMetric;
        }

        @Override
        public void apply(GenericRecord record) {
            Object o = record.get(fieldName);
            if (o instanceof Number) {
                synchronized (this) {
                    value += ((Number) o).doubleValue();
                }
            }
        }

        @Override
        public void merge(SumMetric metric) {
            value += metric.value;
        }
    }

    public static class CountMetric
            extends AbstractMetric<CountMetric> {

        private double value;

        public CountMetric(String fieldName) {
            super(fieldName);
        }

        @Override
        public Number value() {
            return value;
        }

        @Override
        public CountMetric copy() {
            CountMetric countMetric = new CountMetric(fieldName);
            countMetric.value += value;
            return countMetric;
        }

        @Override
        public void apply(GenericRecord record) {
            Object o = record.get(fieldName);
            if (o != null) {
                synchronized (this) {
                    value += 1;
                }
            }
        }

        @Override
        public void merge(CountMetric metric) {
            value += metric.value;
        }
    }

    public static class AverageMetric
            extends AbstractMetric<AverageMetric> {
        private long sum;
        private long count;

        public AverageMetric(String fieldName) {
            super(fieldName);
        }

        @Override
        public Number value() {
            return sum / (double) count;
        }

        @Override
        public AverageMetric copy() {
            AverageMetric averageMetric = new AverageMetric(fieldName);
            averageMetric.count = count;
            averageMetric.sum = sum;
            return averageMetric;
        }

        @Override
        public void apply(GenericRecord record) {
            Object o = record.get(fieldName);
            if (o instanceof Number) {
                synchronized (this) {
                    sum += ((Number) o).doubleValue();
                    count += 1;
                }
            }
        }

        @Override
        public void merge(AverageMetric metric) {
            sum += metric.sum;
            count += metric.count;
        }
    }

    public static class UniqueCountMetric
            extends AbstractMetric<UniqueCountMetric> {
        private Set<Object> set;

        public UniqueCountMetric(String fieldName) {
            super(fieldName);
            set = new HashSet<>();
        }

        @Override
        public Number value() {
            return set.size();
        }

        @Override
        public UniqueCountMetric copy() {
            UniqueCountMetric uniqueCountMetric = new UniqueCountMetric(fieldName);
            uniqueCountMetric.set = set;
            return uniqueCountMetric;
        }

        @Override
        public void apply(GenericRecord record) {
            Object o = record.get(fieldName);
            if (o != null) {
                synchronized (this) {
                    set.add(o);
                }
            }
        }

        @Override
        public void merge(UniqueCountMetric metric) {
            metric.set.addAll(metric.set);
        }
    }

    public static class MaximumMinimumMetric
            extends AbstractMetric<MaximumMinimumMetric> {

        private final boolean maximum;
        private Number value;

        public MaximumMinimumMetric(String fieldName, boolean maximum) {
            super(fieldName);
            this.maximum = maximum;
        }

        @Override
        public Number value() {
            return value;
        }

        @Override
        public MaximumMinimumMetric copy() {
            MaximumMinimumMetric maximumMinimumMetric = new MaximumMinimumMetric(fieldName, maximum);
            maximumMinimumMetric.value = value;
            return maximumMinimumMetric;
        }

        @Override
        public void apply(GenericRecord record) {
            Object o = record.get(fieldName);
            if (o instanceof Number && pass(((Number) o).doubleValue())) {
                synchronized (this) {
                    value = ((Number) o);
                }
            }
        }

        @Override
        public void merge(MaximumMinimumMetric metric) {
            if (pass(metric.value.doubleValue())) {
                value = metric.value;
            }
        }

        private boolean pass(double value) {
            if (this.value == null) {
                return true;
            }
            return maximum ? value > this.value.doubleValue() : value < this.value.doubleValue();
        }
    }

    public static class DummyMetric
            extends AbstractMetric {
        public DummyMetric() {
            super(null);
        }

        @Override
        public Number value() {
            return 0;
        }

        @Override
        public Object copy() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void apply(GenericRecord record) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void merge(Object metric) {
            throw new UnsupportedOperationException();
        }
    }

    public class RealtimeTable {
        public final String project;
        public final String tableName;

        public RealtimeTable(String project, String tableName) {
            this.project = checkNotNull(project, "project is null");
            this.tableName = checkNotNull(tableName, "tableName is null");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof RealtimeTable)) {
                return false;
            }

            RealtimeTable that = (RealtimeTable) o;

            if (!project.equals(that.project)) {
                return false;
            }
            return tableName.equals(that.tableName);
        }

        @Override
        public int hashCode() {
            int result = project.hashCode();
            result = 31 * result + tableName.hashCode();
            return result;
        }
    }
}
