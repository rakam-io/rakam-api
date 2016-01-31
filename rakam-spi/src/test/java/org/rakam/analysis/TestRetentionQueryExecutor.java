package org.rakam.analysis;

import com.google.common.collect.ImmutableMap;
import org.rakam.EventBuilder;
import org.rakam.analysis.RetentionQueryExecutor.RetentionAction;
import org.rakam.collection.Event;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.plugin.EventStore;
import org.rakam.report.QueryResult;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.of;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.util.Arrays.asList;
import static org.rakam.analysis.RetentionQueryExecutor.DateUnit.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public abstract class TestRetentionQueryExecutor {
    private static final int SCALE_FACTOR = 3;

    @BeforeSuite
    public void setup() throws Exception {
        EventBuilder builder = new EventBuilder("test", getMetastore());

        getMetastore().createProject("test");
        for (int cIdx = 0; cIdx < 2; cIdx ++) {
            final int finalCIdx = cIdx;
            List<Event> events = IntStream.range(0, SCALE_FACTOR).mapToObj(i -> builder.createEvent("test" + finalCIdx,
                    ImmutableMap.<String, Object>builder()
                            .put("teststr", "test" + (i % 2))
                            .put("_user", "test" + (i % 2))
                            .put("_time", Instant.ofEpochSecond(i * DAYS.getDuration().getSeconds())).build())).collect(Collectors.toList());

            getEventStore().storeBatch(events);
        }
    }

    public abstract EventStore getEventStore();

    public abstract Metastore getMetastore();

    public abstract RetentionQueryExecutor getRetentionQueryExecutor();

    @Test
    public void testSimpleRetentionQuery() throws Exception {
        QueryResult result = getRetentionQueryExecutor().query("test", "_user", Optional.empty(), Optional.empty(), DAY, Optional.empty(),
                LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR)).getResult().join();

        assertFalse(result.isFailed());
        assertEquals(of(
                asList(LocalDate.parse("1970-01-01"), null, 1L),
                asList(LocalDate.parse("1970-01-01"), 0L, 1L),
                asList(LocalDate.parse("1970-01-01"), 2L, 1L),
                asList(LocalDate.parse("1970-01-02"), null, 1L),
                asList(LocalDate.parse("1970-01-02"), 0L, 1L),
                asList(LocalDate.parse("1970-01-03"), null, 1L),
                asList(LocalDate.parse("1970-01-03"), 0L, 1L)), result.getResult());
    }

    @Test
    public void testDifferentCollections() throws Exception {
        QueryResult result = getRetentionQueryExecutor().query("test", "_user",
                Optional.of(RetentionAction.create("test0", Optional.empty())),
                Optional.of(RetentionAction.create("test1", Optional.empty())), DAY, Optional.empty(),
                LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR))
                .getResult().join();

        assertFalse(result.isFailed());
        assertEquals(result.getResult(), of(
                asList(LocalDate.parse("1970-01-01"), null, 1L),
                asList(LocalDate.parse("1970-01-01"), 2L, 1L),
                asList(LocalDate.parse("1970-01-02"), null, 1L),
                asList(LocalDate.parse("1970-01-03"), null, 1L)));
    }

    @Test
    public void testFilter() throws Exception {
        QueryResult result = getRetentionQueryExecutor().query("test", "_user",
                Optional.of(RetentionAction.create("test0", Optional.of("teststr = 'test0'"))),
                Optional.of(RetentionAction.create("test1", Optional.of("teststr = 'test0'"))),
                DAY, Optional.empty(),
                LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR))
                .getResult().join();

        assertFalse(result.isFailed());
        assertEquals(result.getResult(), of(
                asList(LocalDate.parse("1970-01-01"), null, 1L),
                asList(LocalDate.parse("1970-01-01"), 2L, 1L),
                asList(LocalDate.parse("1970-01-03"), null, 1L)));
    }

    @Test
    public void testDimension() throws Exception {
        QueryResult result = getRetentionQueryExecutor().query("test", "_user",
                Optional.of(RetentionAction.create("test0", Optional.empty())),
                Optional.of(RetentionAction.create("test1", Optional.empty())),
                DAY, Optional.of("teststr"),
                LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR)).getResult().join();

        assertFalse(result.isFailed());
        assertEquals(result.getResult(), of(
                asList("test0", null, 1L),
                asList("test0", 2L, 1L),
                asList("test1", null, 1L)));
    }

    @Test
    public void testTimeRange() throws Exception {
        QueryResult result = getRetentionQueryExecutor().query("test", "_user",
                Optional.empty(),
                Optional.empty(),
                DAY, Optional.of("teststr"),
                LocalDate.ofEpochDay(10000), LocalDate.ofEpochDay(10000))
                .getResult().join();

        assertFalse(result.isFailed());
        assertEquals(result.getResult(), of());
    }

    @Test
    public void testWeeklyRetention() throws Exception {
        QueryResult result = getRetentionQueryExecutor().query("test", "_user",
                Optional.empty(),
                Optional.empty(),
                WEEK, Optional.of("teststr"),
                LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR))
                .getResult().join();

        assertFalse(result.isFailed());
        assertEquals(result.getResult(), of(
                asList(LocalDate.parse("1969-12-29"), null, 2L), // week start
                asList(LocalDate.parse("1969-12-29"), 0L, 2L)));
    }

    @Test
    public void testMonthlyRetention() throws Exception {
        QueryResult result = getRetentionQueryExecutor().query("test", "_user",
                Optional.empty(),
                Optional.empty(),
                MONTH, Optional.of("teststr"),
                LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR))
                .getResult().join();

        assertFalse(result.isFailed());
        assertEquals(result.getResult(), of(
                asList(LocalDate.parse("1970-01-01"), null, 2L),
                asList(LocalDate.parse("1970-01-01"), 0L, 2L)));
    }
}
