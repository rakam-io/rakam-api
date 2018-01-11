package org.rakam.analysis;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.rakam.EventBuilder;
import org.rakam.analysis.EventExplorer.TimestampTransformation;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.Event;
import org.rakam.plugin.EventStore;
import org.rakam.report.QueryResult;
import org.rakam.report.realtime.AggregationType;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.time.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.ImmutableSet.copyOf;
import static org.rakam.analysis.EventExplorer.ReferenceType.COLUMN;
import static org.rakam.analysis.EventExplorer.TimestampTransformation.*;
import static org.rakam.report.realtime.AggregationType.*;
import static org.testng.Assert.*;

public abstract class TestEventExplorer {
    protected static final String PROJECT_NAME = "2";
    private static final int SCALE_FACTOR = 100;
    private static final Map<TimestampTransformation, Set<List>> EVENT_STATISTICS_RESULTS = ImmutableMap.<TimestampTransformation, Set<List>>builder()
            .put(HOUR_OF_DAY, ImmutableSet.of(of("test", "00:00", 36L), of("test", "01:00", 36L), of("test", "02:00", 28L)))
            .put(DAY_OF_MONTH, ImmutableSet.of(of("test", "1th day", 100L)))
            .put(WEEK_OF_YEAR, ImmutableSet.of(of("test", "1th week", 100L)))
            .put(MONTH_OF_YEAR, ImmutableSet.of(of("test", "January", 100L)))
            .put(QUARTER_OF_YEAR, ImmutableSet.of(of("test", "1th quarter", 100L)))
            .put(DAY_OF_WEEK, ImmutableSet.of(of("test", "Thursday", 100L)))
            .put(HOUR, ImmutableSet.of(of("test", parse("1970-01-01T00:00:00Z"), 36L), of("test", parse("1970-01-01T01:00:00Z"), 36L), of("test", parse("1970-01-01T02:00:00Z"), 28L)))
            .put(DAY, ImmutableSet.of(of("test", LocalDate.parse("1970-01-01"), 100L)))
            .put(WEEK, ImmutableSet.of(of("test", LocalDate.parse("1969-12-29"), 100L)))
            .put(MONTH, ImmutableSet.of(of("test", LocalDate.parse("1970-01-01"), 100L)))
            .put(YEAR, ImmutableSet.of(of("test", LocalDate.parse("1970-01-01"), 100L))).build();

    private static ZonedDateTime parse(String value) {
        return ZonedDateTime.parse(value).withZoneSameLocal(ZoneId.of("UTC"));
    }

    @BeforeSuite
    public void setup()
            throws Exception {
        getMetastore().createProject(PROJECT_NAME);

        EventBuilder builder = new EventBuilder(PROJECT_NAME, getMetastore());

        List<Event> events = IntStream.range(0, SCALE_FACTOR).mapToObj(i -> builder.createEvent("test", ImmutableMap.<String, Object>builder()
                .put("teststr", "test" + i)
                .put("testnumber", (double) i)
                .put("testbool", i % 2 == 0)
                .put("testmap", ImmutableMap.of("test" + i, (double) i))
                .put("testarray", of((double) i))
                .put("testdate", LocalDate.ofEpochDay(i))
                .put("_time", Instant.ofEpochSecond(i * 100)).build()))
                .collect(Collectors.toList());

        getEventStore().storeBatch(events);
    }

    @AfterSuite
    public void destroy() {
        getMetastore().deleteProject(PROJECT_NAME);
    }

    public abstract EventStore getEventStore();

    public abstract Metastore getMetastore();

    public abstract EventExplorer getEventExplorer();

    @Test
    public void testTotalStatistics()
            throws Exception {
        QueryResult test = getEventExplorer().getEventStatistics(new RequestContext(PROJECT_NAME, null),
                Optional.empty(), Optional.empty(),
                LocalDate.ofEpochDay(0),
                LocalDate.ofEpochDay(SCALE_FACTOR), ZoneOffset.UTC).join();

        assertFalse(test.isFailed());
        assertEquals(copyOf(test.getResult()), ImmutableSet.of(of("test", 100L)));
    }

    @Test
    public void testCollectionSingleName()
            throws Exception {
        QueryResult test = getEventExplorer().getEventStatistics(new RequestContext(PROJECT_NAME, null),
                Optional.of(ImmutableSet.of("test")), Optional.empty(),
                LocalDate.ofEpochDay(0),
                LocalDate.ofEpochDay(SCALE_FACTOR), ZoneOffset.UTC).join();

        assertFalse(test.isFailed());
        assertEquals(copyOf(test.getResult()), ImmutableSet.of(of("test", 100L)));
    }

    @Test
    public void testCollectionNotExisting()
            throws Exception {
        QueryResult test = getEventExplorer().getEventStatistics(new RequestContext(PROJECT_NAME, null),
                Optional.of(ImmutableSet.of()), Optional.empty(),
                LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR), ZoneOffset.UTC).join();

        assertFalse(test.isFailed());
        assertEquals(test.getResult(), of());
    }

    @Test
    public void testExtraDimensionsForStatistics()
            throws Exception {
        Collection<List<String>> dimensions = getEventExplorer().getExtraDimensions("test").values();
        dimensions.stream().flatMap(e -> e.stream()).forEach(dimension -> {
            QueryResult test = getEventExplorer().getEventStatistics(new RequestContext(PROJECT_NAME, null),
                    Optional.empty(), Optional.of(dimension),
                    LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR), ZoneOffset.UTC).join();

            assertFalse(test.isFailed());

            Optional<TimestampTransformation> transformation = fromPrettyName(dimension);
            if (transformation.isPresent()) {
                assertEquals(copyOf(test.getResult()), EVENT_STATISTICS_RESULTS.get(transformation.get()));
            } else {
                // TODO: test custom parameters
            }
        });
    }

    @Test
    public void testStatisticsDates()
            throws Exception {
        QueryResult test = getEventExplorer().getEventStatistics(new RequestContext(PROJECT_NAME, null),
                Optional.empty(), Optional.empty(),
                LocalDate.ofEpochDay(100),
                LocalDate.ofEpochDay(101), ZoneOffset.UTC).join();

        assertTrue(!test.isFailed(),
                test.getError() != null ? test.getError().toString() : null);
        assertEquals(test.getResult(), of());
    }

    @Test
    public void testAllDimensionsNumberBoolean()
            throws Exception {
        QueryResult test = getEventExplorer().analyze(new RequestContext(PROJECT_NAME, null),
                of("test"), new EventExplorer.Measure(null, COUNT),
                new EventExplorer.Reference(COLUMN, "testnumber"),
                new EventExplorer.Reference(COLUMN, "testbool"),
                null, LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR), ZoneOffset.UTC).getResult().join();

        assertFalse(test.isFailed());
        assertEquals(test.getResult().size(), 17);

        assertEquals(test.getResult().get(0).get(0), "Others");
        assertEquals(test.getResult().get(1).get(0), "Others");

        assertEquals(test.getResult().stream().mapToLong(a -> ((Number) a.get(2)).longValue()).sum(), 100L);

        for (int i = 2; i < test.getResult().size(); i++) {
            assertTrue(ImmutableSet.of("true", "false").contains(test.getResult().get(i).get(1)));
            assertEquals(test.getResult().get(i).get(2), 1L);
        }
    }

    @Test
    public void testGroupingNumberBoolean()
            throws Exception {
        QueryResult test = getEventExplorer().analyze(new RequestContext(PROJECT_NAME, null),
                of("test"), new EventExplorer.Measure(null, COUNT),
                new EventExplorer.Reference(COLUMN, "testnumber"),
                null,
                null,
                LocalDate.ofEpochDay(0),
                LocalDate.ofEpochDay(SCALE_FACTOR), ZoneOffset.UTC)
                .getResult().join();

        assertFalse(test.isFailed());
        assertEquals(test.getResult().size(), 16);

        assertEquals(test.getResult().get(0), ImmutableList.of("Others", "test", 85L));
        assertEquals(test.getResult().stream().mapToLong(a -> (Long) a.get(2)).sum(), 100L);
    }

    @Test
    public void testSimpleWithFilter()
            throws Exception {
        QueryResult test = getEventExplorer().analyze(new RequestContext(PROJECT_NAME, null),
                of("test"), new EventExplorer.Measure(null, COUNT),
                null,
                null,
                "testbool", LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR), ZoneOffset.UTC).getResult().join();

        assertFalse(test.isFailed());
        assertEquals(test.getResult().get(0), of("test", 50L));
    }

    @Test
    public void testSimple()
            throws Exception {
        QueryResult test = getEventExplorer().analyze(new RequestContext(PROJECT_NAME, null),
                of("test"), new EventExplorer.Measure(null, COUNT),
                null,
                null,
                null, LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR), ZoneOffset.UTC).getResult().join();

        assertFalse(test.isFailed(), test.isFailed() ? test.getError().message : null);
        assertEquals(test.getResult().get(0), of("test", 100L));
    }

    @Test
    public void testSumAggregation()
            throws Exception {
        QueryResult test = getEventExplorer().analyze(new RequestContext(PROJECT_NAME, null),
                of("test"), new EventExplorer.Measure("testnumber", SUM),
                null,
                null,
                null, LocalDate.ofEpochDay(0),
                LocalDate.ofEpochDay(SCALE_FACTOR), ZoneOffset.UTC).getResult().join();

        assertFalse(test.isFailed());
        assertEquals(test.getResult().get(0), of("test", 4950.0));
    }

    @Test
    public void testInvalidAvgAggregation()
            throws Exception {
        QueryResult test = getEventExplorer().analyze(new RequestContext(PROJECT_NAME, null),
                of("test"), new EventExplorer.Measure("teststr", AVERAGE),
                new EventExplorer.Reference(COLUMN, "testbool"),
                null,
                null, LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR), ZoneOffset.UTC).getResult().join();

        assertTrue(test.isFailed());
    }

    @Test
    public void testAvgAggregation()
            throws Exception {
        QueryResult test = getEventExplorer().analyze(new RequestContext(PROJECT_NAME, null),
                of("test"), new EventExplorer.Measure("testnumber", AVERAGE),
                new EventExplorer.Reference(COLUMN, "testbool"),
                null,
                null, LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR), ZoneOffset.UTC).getResult().join();

        assertFalse(test.isFailed());
        assertEquals(copyOf(test.getResult()), ImmutableSet.of(of("true", "test", 49.0), of("false", "test", 50.0)));
    }

    @Test
    public void testMaximumAggregation()
            throws Exception {
        QueryResult test = getEventExplorer().analyze(new RequestContext(PROJECT_NAME, null),
                of("test"), new EventExplorer.Measure("testnumber", MAXIMUM),
                new EventExplorer.Reference(COLUMN, "testbool"),
                null,
                null, LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR), ZoneOffset.UTC).getResult().join();

        assertFalse(test.isFailed());
        assertEquals(copyOf(test.getResult()), ImmutableSet.of(of("true", "test", 98.0), of("false", "test", 99.0)));
    }

    @Test
    public void testSegmentAggregation()
            throws Exception {
        QueryResult test = getEventExplorer().analyze(new RequestContext(PROJECT_NAME, null),
                of("test"), new EventExplorer.Measure("testnumber", AggregationType.COUNT_UNIQUE),
                new EventExplorer.Reference(COLUMN, "testbool"),
                new EventExplorer.Reference(COLUMN, "testbool"),
                null, LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR), ZoneOffset.UTC).getResult().join();

        assertFalse(test.isFailed());
        assertEquals(copyOf(test.getResult()), ImmutableSet.of(of("true", "true", 50L), of("false", "false", 50L)));
    }

    @Test
    public void testCountUniqueAggregation()
            throws Exception {
        QueryResult test = getEventExplorer().analyze(new RequestContext(PROJECT_NAME, null),
                of("test"), new EventExplorer.Measure("testnumber", AggregationType.COUNT_UNIQUE),
                new EventExplorer.Reference(COLUMN, "testbool"),
                null,
                null, LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR), ZoneOffset.UTC).getResult().join();

        assertFalse(test.isFailed());
        assertEquals(copyOf(test.getResult()), ImmutableSet.of(of("true", "test", 50L), of("false", "test", 50L)));
    }

    @Test
    public void testCountAggregation()
            throws Exception {
        QueryResult test = getEventExplorer().analyze(new RequestContext(PROJECT_NAME, null),
                of("test"), new EventExplorer.Measure("testnumber", COUNT),
                new EventExplorer.Reference(COLUMN, "testbool"),
                null,
                null, LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR), ZoneOffset.UTC).getResult().join();

        assertFalse(test.isFailed());
        assertEquals(copyOf(test.getResult()), ImmutableSet.of(of("true", "test", 50L), of("false", "test", 50L)));
    }

    @Test
    public void testMinimumAggregation()
            throws Exception {
        QueryResult test = getEventExplorer().analyze(new RequestContext(PROJECT_NAME, null),
                of("test"), new EventExplorer.Measure("testnumber", MINIMUM),
                new EventExplorer.Reference(COLUMN, "testbool"),
                null,
                null, LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR), ZoneOffset.UTC).getResult().join();

        assertFalse(test.isFailed());
        assertEquals(copyOf(test.getResult()), ImmutableSet.of(of("true", "test", 0.0), of("false", "test", 1.0)));
    }

    @Test
    public void testApproxAggregation()
            throws Exception {
        QueryResult test = getEventExplorer().analyze(new RequestContext(PROJECT_NAME, null),
                of("test"), new EventExplorer.Measure("teststr", AggregationType.APPROXIMATE_UNIQUE),
                new EventExplorer.Reference(COLUMN, "testbool"),
                null,
                null, LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR), ZoneOffset.UTC).getResult().join();

        assertFalse(test.isFailed());
        assertEquals(copyOf(test.getResult()), ImmutableSet.of(of("true", "test", 50L), of("false", "test", 50L)));
    }

    @Test
    public void testReferenceGrouping()
            throws Exception {
        Map<TimestampTransformation, Set> GROUPING = ImmutableMap.<TimestampTransformation, Set>builder()
                .put(HOUR_OF_DAY, ImmutableSet.of(of("00:00", "test", 36L), of("01:00", "test", 36L), of("02:00", "test", 28L)))
                .put(DAY_OF_MONTH, ImmutableSet.of(of("1th day", "test", 100L)))
                .put(WEEK_OF_YEAR, ImmutableSet.of(of("1th week", "test", 100L)))
                .put(MONTH_OF_YEAR, ImmutableSet.of(of("January", "test", 100L)))
                .put(QUARTER_OF_YEAR, ImmutableSet.of(of("1th quarter", "test", 100L)))
                .put(DAY_OF_WEEK, ImmutableSet.of(of("Thursday", "test", 100L)))
                .put(HOUR, ImmutableSet.of(of(parse("1970-01-01T00:00:00Z"), "test", 36L), of(parse("1970-01-01T01:00:00Z"), "test", 36L), of(parse("1970-01-01T02:00:00Z"), "test", 28L)))
                .put(DAY, ImmutableSet.of(of(LocalDate.parse("1970-01-01"), "test", 100L)))
                .put(WEEK, ImmutableSet.of(of(LocalDate.parse("1969-12-29"), "test", 100L)))
                .put(MONTH, ImmutableSet.of(of(LocalDate.parse("1970-01-01"), "test", 100L)))
                .put(YEAR, ImmutableSet.of(of(LocalDate.parse("1970-01-01"), "test", 100L)))
                .build();

        getEventExplorer().getExtraDimensions("test").values().stream().flatMap(e -> e.stream())
                .forEach(dimension -> {
                    Optional<TimestampTransformation> trans = fromPrettyName(dimension);

                    if (trans.isPresent()) {
                        QueryResult test = getEventExplorer().analyze(new RequestContext(PROJECT_NAME, null),
                                of("test"), new EventExplorer.Measure("teststr", AggregationType.APPROXIMATE_UNIQUE),
                                new EventExplorer.Reference(EventExplorer.ReferenceType.REFERENCE, trans.get().getPrettyName()),
                                null,
                                null, LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR), ZoneOffset.UTC).getResult().join();

                        assertFalse(test.isFailed());
                        assertEquals(copyOf(test.getResult()), GROUPING.get(trans.get()));
                    } else {
                        // TODO: implement
                    }
                });
    }

    @Test
    public void testMultipleReferenceGrouping()
            throws Exception {
        QueryResult test = getEventExplorer().analyze(new RequestContext(PROJECT_NAME, null),
                of("test"), new EventExplorer.Measure("teststr", AggregationType.APPROXIMATE_UNIQUE),
                new EventExplorer.Reference(EventExplorer.ReferenceType.REFERENCE, DAY_OF_MONTH.getPrettyName()),
                new EventExplorer.Reference(EventExplorer.ReferenceType.REFERENCE, DAY_OF_MONTH.getPrettyName()),
                null, LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR), ZoneOffset.UTC).getResult().join();

        assertFalse(test.isFailed());
        assertEquals(copyOf(test.getResult()), ImmutableSet.of(of("1th day", "1th day", 100L)));
    }
}

