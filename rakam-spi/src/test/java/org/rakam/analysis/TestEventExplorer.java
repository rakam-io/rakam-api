package org.rakam.analysis;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.rakam.EventBuilder;
import org.rakam.collection.Event;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.plugin.EventStore;
import org.rakam.realtime.AggregationType;
import org.rakam.report.QueryResult;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.of;
import static org.rakam.analysis.EventExplorer.ReferenceType.COLUMN;
import static org.rakam.analysis.EventExplorer.TimestampTransformation.*;
import static org.testng.Assert.*;

public abstract class TestEventExplorer {
    private static final int SCALE_FACTOR = 100;

    @BeforeSuite
    public void addEvents() throws Exception {

        EventBuilder builder = new EventBuilder("test", getMetastore());

        getMetastore().createProject("test");
        List<Event> events = IntStream.range(0, SCALE_FACTOR).mapToObj(i -> builder.createEvent("test", ImmutableMap.<String, Object>builder()
                .put("teststr", "test" + i)
                .put("testnumber", i)
                .put("testbool", i % 2 == 0)
                .put("testmap", ImmutableMap.of("test"+i, i))
                .put("testarray", of(i))
                .put("testdate", LocalDate.ofEpochDay(i))
                .put("_time", Instant.ofEpochSecond(i * 100)).build())).collect(Collectors.toList());

        getEventStore().storeBatch(events);
    }

    public abstract EventStore getEventStore();

    public abstract Metastore getMetastore();

    public abstract EventExplorer getEventExplorer();

    private static final Map<EventExplorer.TimestampTransformation, Set<List>> EVENT_STATISTICS_RESULTS = ImmutableMap.<EventExplorer.TimestampTransformation, Set<List>>builder()
            .put(HOUR_OF_DAY, ImmutableSet.of(of("test", 0L, 36L), of("test", 1L, 36L), of("test", 2L, 28L)))
            .put(DAY_OF_MONTH, ImmutableSet.of(of("test", 1L, 100L)))
            .put(WEEK_OF_YEAR, ImmutableSet.of(of("test", 1L, 100L)))
            .put(MONTH_OF_YEAR, ImmutableSet.of(of("test", 1L, 100L)))
            .put(QUARTER_OF_YEAR, ImmutableSet.of(of("test", 1L, 100L)))
            .put(DAY_OF_WEEK, ImmutableSet.of(of("test", 4L, 100L)))
            .put(HOUR, ImmutableSet.of(of("test", Instant.parse("1970-01-01T00:00:00Z"), 36L), of("test", Instant.parse("1970-01-01T01:00:00Z"), 36L), of("test", Instant.parse("1970-01-01T02:00:00Z"), 28L)))
            .put(DAY, ImmutableSet.of(of("test", "1970-01-01", 100L)))
            .put(WEEK, ImmutableSet.of(of("test", Instant.parse("1969-12-29T00:00:00Z"), 100L)))
            .put(MONTH, ImmutableSet.of(of("test", Instant.parse("1970-01-01T00:00:00Z"), 100L)))
            .put(YEAR, ImmutableSet.of(of("test", Instant.parse("1970-01-01T00:00:00Z"), 100L))).build();

    @Test
    public void testTotalStatistics() throws Exception {
        QueryResult test = getEventExplorer().getEventStatistics("test",
                Optional.empty(), Optional.empty(),
                LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR)).join();
        assertEquals(ImmutableSet.copyOf(test.getResult()), ImmutableSet.of(of("test", 100L)));
    }

    @Test
    public void testCollectionSingleName() throws Exception {
        QueryResult test = getEventExplorer().getEventStatistics("test",
                Optional.of(ImmutableSet.of("test")), Optional.empty(),
                LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR)).join();
        assertEquals(ImmutableSet.copyOf(test.getResult()), ImmutableSet.of(of("test", 100L)));
    }

    @Test
    public void testCollectionNotExisting() throws Exception {
        QueryResult test = getEventExplorer().getEventStatistics("test",
                Optional.of(ImmutableSet.of()), Optional.empty(),
                LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR)).join();
        assertEquals(test.getResult(), of());
    }

    @Test
    public void testExtraDimensions() throws Exception {
        List<String> dimensions = getEventExplorer().getExtraDimensions("test");
        for (String dimension : dimensions) {
            QueryResult test = getEventExplorer().getEventStatistics("test",
                    Optional.empty(), Optional.of(dimension),
                    LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR)).join();

            Optional<EventExplorer.TimestampTransformation> transformation = EventExplorer.TimestampTransformation.fromPrettyName(dimension);

            assertFalse(test.isFailed());

            if(transformation.isPresent()) {
                assertEquals(EVENT_STATISTICS_RESULTS.get(transformation.get()), ImmutableSet.copyOf(test.getResult()));
            } else {
                // TODO: test custom parameters
            }
        }
    }

    @Test
    public void testStatisticsDates() throws Exception {
        QueryResult test = getEventExplorer().getEventStatistics("test",
                Optional.empty(), Optional.empty(),
                LocalDate.ofEpochDay(100), LocalDate.ofEpochDay(101)).join();

        assertEquals(test.getResult(), of());
    }

    @Test
    public void testAllDimensionsNumberBoolean() throws Exception {
        QueryResult test = getEventExplorer().analyze("test",
                of("test"), new EventExplorer.Measure(null, AggregationType.COUNT),
                new EventExplorer.Reference(COLUMN, "testnumber"),
                new EventExplorer.Reference(COLUMN, "testbool"),
                null, LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR)).join();

        assertEquals(test.getResult().size(), 17);

        assertEquals(test.getResult().get(0).get(0), "Others");
        assertEquals(test.getResult().get(1).get(0), "Others");

        assertEquals(test.getResult().stream().mapToLong(a -> (Long) a.get(2)).sum(), 100L);

        for (int i = 2; i < test.getResult().size(); i++) {
            assertTrue(ImmutableSet.of("true", "false").contains(test.getResult().get(i).get(1)));
            assertEquals(test.getResult().get(i).get(2), 1L);
        }
    }

    @Test
    public void testGroupingNumberBoolean() throws Exception {
        QueryResult test = getEventExplorer().analyze("test",
                of("test"), new EventExplorer.Measure(null, AggregationType.COUNT),
                new EventExplorer.Reference(COLUMN, "testnumber"),
                null,
                null, LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR)).join();

        assertEquals(test.getResult().size(), 51);

        assertEquals(test.getResult().get(0), ImmutableList.of("Others", 50L));
        assertEquals(test.getResult().stream().mapToLong(a -> (Long) a.get(1)).sum(), 100L);
    }

    @Test
    public void testSimpleWithFilter() throws Exception {
        QueryResult test = getEventExplorer().analyze("test",
                of("test"), new EventExplorer.Measure(null, AggregationType.COUNT),
                null,
                null,
                "testbool", LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR)).join();

        assertEquals(test.getResult().get(0), of(50L));
    }

    @Test
    public void testSimple() throws Exception {
        QueryResult test = getEventExplorer().analyze("test",
                of("test"), new EventExplorer.Measure(null, AggregationType.COUNT),
                null,
                null,
                null, LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR)).join();

        assertEquals(test.getResult().get(0), of(100L));
    }

    @Test
    public void testSumAggregation() throws Exception {
        QueryResult test = getEventExplorer().analyze("test",
                of("test"), new EventExplorer.Measure("testnumber", AggregationType.SUM),
                null,
                null,
                null, LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR)).join();

        assertEquals(test.getResult().get(0), 4950.0);
    }

    @Test
    public void testInvalidAvgAggregation() throws Exception {
        QueryResult test = getEventExplorer().analyze("test",
                of("test"), new EventExplorer.Measure("teststr", AggregationType.AVERAGE),
                new EventExplorer.Reference(EventExplorer.ReferenceType.COLUMN, "testbool"),
                null,
                null, LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR)).join();

        assertTrue(test.isFailed());
    }

    @Test
    public void testAvgAggregation() throws Exception {
        QueryResult test = getEventExplorer().analyze("test",
                of("test"), new EventExplorer.Measure("testnumber", AggregationType.AVERAGE),
                new EventExplorer.Reference(EventExplorer.ReferenceType.COLUMN, "testbool"),
                null,
                null, LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR)).join();

        assertEquals(ImmutableSet.copyOf(test.getResult()), ImmutableSet.of(of("true", 49L), of("false", 50L)));
    }

    @Test
    public void testMaximumAggregation() throws Exception {
        QueryResult test = getEventExplorer().analyze("test",
                of("test"), new EventExplorer.Measure("testnumber", AggregationType.MAXIMUM),
                new EventExplorer.Reference(EventExplorer.ReferenceType.COLUMN, "testbool"),
                null,
                null, LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR)).join();

        assertEquals(ImmutableSet.copyOf(test.getResult()), ImmutableSet.of(of("true", 99.0), of("false", 99.0)));
    }

    @Test
    public void testSegmentAggregation() throws Exception {
        QueryResult test = getEventExplorer().analyze("test",
                of("test"), new EventExplorer.Measure("testnumber", AggregationType.COUNT_UNIQUE),
                new EventExplorer.Reference(EventExplorer.ReferenceType.COLUMN, "testbool"),
                new EventExplorer.Reference(EventExplorer.ReferenceType.COLUMN, "testnumber"),
                null, LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR)).join();

        assertEquals(ImmutableSet.copyOf(test.getResult()), ImmutableSet.of(of("true", 98L), of("false", 99L)));
    }

    @Test
    public void testCountUniqueAggregation() throws Exception {
        QueryResult test = getEventExplorer().analyze("test",
                of("test"), new EventExplorer.Measure("testnumber", AggregationType.COUNT_UNIQUE),
                new EventExplorer.Reference(EventExplorer.ReferenceType.COLUMN, "testbool"),
                null,
                null, LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR)).join();

        assertEquals(ImmutableSet.copyOf(test.getResult()), ImmutableSet.of(of("true", 50L), of("false", 50L)));
    }

    @Test
    public void testCountAggregation() throws Exception {
        QueryResult test = getEventExplorer().analyze("test",
                of("test"), new EventExplorer.Measure("testnumber", AggregationType.COUNT),
                new EventExplorer.Reference(EventExplorer.ReferenceType.COLUMN, "testbool"),
                null,
                null, LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR)).join();

        assertEquals(ImmutableSet.copyOf(test.getResult()), ImmutableSet.of(of("true", 50L), of("false", 50L)));
    }

    @Test
    public void testMinimumAggregation() throws Exception {
        QueryResult test = getEventExplorer().analyze("test",
                of("test"), new EventExplorer.Measure("testnumber", AggregationType.MINIMUM),
                new EventExplorer.Reference(EventExplorer.ReferenceType.COLUMN, "testbool"),
                null,
                null, LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR)).join();

        assertEquals(ImmutableSet.copyOf(test.getResult()), ImmutableSet.of(of("true", 0.0), of("false", 1.0)));
    }

    @Test
    public void testApproxAggregation() throws Exception {
        QueryResult test = getEventExplorer().analyze("test",
                of("test"), new EventExplorer.Measure("teststr", AggregationType.APPROXIMATE_UNIQUE),
                new EventExplorer.Reference(EventExplorer.ReferenceType.COLUMN, "testbool"),
                null,
                null, LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR)).join();

        assertEquals(ImmutableSet.copyOf(test.getResult()), ImmutableSet.of(of("true", 50L), of("false", 50L)));
    }

    @Test
    public void testReferenceGrouping() throws Exception {
        Map<EventExplorer.TimestampTransformation, Set> GROUPING = ImmutableMap.<EventExplorer.TimestampTransformation, Set>builder()
                .put(HOUR_OF_DAY, ImmutableSet.of(of(1L, 36L), of(0L, 36L), of(2L, 28L)))
                .put(DAY_OF_MONTH, ImmutableSet.of(of(1L, 100L)))
                .put(WEEK_OF_YEAR, ImmutableSet. of(of(1L, 100L)))
                .put(MONTH_OF_YEAR, ImmutableSet.of(of(1L, 100L)))
                .put(QUARTER_OF_YEAR, ImmutableSet. of(of(1L, 100L)))
                .put(DAY_OF_WEEK, ImmutableSet.of(of(4L, 100L)))
                .put(HOUR, ImmutableSet.of(of(Instant.parse("1970-01-01T00:00:00Z"), 36L), of(Instant.parse("1970-01-01T01:00:00Z"), 36L), of(Instant.parse("1970-01-01T02:00:00Z"), 28L)))
                .put(DAY, ImmutableSet.of(of("1970-01-01", 100L)))
                .put(WEEK, ImmutableSet.of(of(Instant.parse("1969-12-29T00:00:00Z"), 100L)))
                .put(MONTH, ImmutableSet.of(of(Instant.parse("1970-01-01T00:00:00Z"), 100L)))
                .put(YEAR, ImmutableSet.of(of(Instant.parse("1970-01-01T00:00:00Z"), 100L)))
                .build();

        List<String> dimensions = getEventExplorer().getExtraDimensions("test");

        for (String dimension : dimensions) {
            Optional<EventExplorer.TimestampTransformation> trans = EventExplorer.TimestampTransformation.fromPrettyName(dimension);

            if(trans.isPresent()) {
                QueryResult test = getEventExplorer().analyze("test",
                        of("test"), new EventExplorer.Measure("teststr", AggregationType.APPROXIMATE_UNIQUE),
                        new EventExplorer.Reference(EventExplorer.ReferenceType.REFERENCE, trans.get().getPrettyName()),
                        null,
                        null, LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR)).join();

                assertEquals(GROUPING.get(trans.get()), ImmutableSet.copyOf(test.getResult()));
            } else {
                // TODO: implement
            }

        }
    }

    @Test
    public void tesMultipleReferenceGrouping() throws Exception {
        QueryResult test = getEventExplorer().analyze("test",
                of("test"), new EventExplorer.Measure("teststr", AggregationType.APPROXIMATE_UNIQUE),
                new EventExplorer.Reference(EventExplorer.ReferenceType.REFERENCE, DAY_OF_MONTH.getPrettyName()),
                new EventExplorer.Reference(EventExplorer.ReferenceType.REFERENCE, DAY_OF_MONTH.getPrettyName()),
                null, LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR)).join();

        assertEquals(ImmutableSet.copyOf(test.getResult()), ImmutableSet.of(of(1L, 1L, 100L)));
    }
}

