package org.rakam.analysis;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.rakam.EventBuilder;
import org.rakam.analysis.RetentionQueryExecutor.RetentionAction;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.Event;
import org.rakam.plugin.EventStore;
import org.rakam.report.QueryResult;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
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
    private static final String PROJECT_NAME = TestRetentionQueryExecutor.class.getName().replace(".", "_").toLowerCase();
    private static final RequestContext CONTEXT = new RequestContext(PROJECT_NAME, null);

    @BeforeSuite
    public void setup() throws Exception {
        EventBuilder builder = new EventBuilder(PROJECT_NAME, getMetastore());

        getMetastore().createProject(PROJECT_NAME);
        for (int cIdx = 0; cIdx < 2; cIdx++) {
            final int finalCIdx = cIdx;
            List<Event> events = IntStream.range(0, SCALE_FACTOR).mapToObj(i -> builder.createEvent("test" + finalCIdx,
                    ImmutableMap.<String, Object>builder()
                            .put("teststr", "test" + (i % 2))
                            .put("_user", "test" + (i % 2))
                            .put("_time", Instant.ofEpochSecond(i * DAYS.getDuration().getSeconds())).build()))
                    .collect(Collectors.toList());

            getEventStore().storeBatch(events);
        }
    }

    @AfterSuite
    protected void clean() {
        getMetastore().deleteProject(PROJECT_NAME);

    }

    public abstract EventStore getEventStore();

    public abstract Metastore getMetastore();

    public abstract RetentionQueryExecutor getRetentionQueryExecutor();

    @Test
    public void testSimpleRetentionQuery() throws Exception {
        QueryResult result = getRetentionQueryExecutor().query(CONTEXT, Optional.empty(), Optional.empty(), DAY, Optional.empty(),
                Optional.of(15), LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR), ZoneOffset.UTC, false)
                .getResult().join();

        assertFalse(result.isFailed());
        assertEquals(result.getResult(), of(
                asList(LocalDate.parse("1970-01-01"), null, 1L),
                asList(LocalDate.parse("1970-01-01"), 1L, 1L),
                asList(LocalDate.parse("1970-01-02"), null, 1L),
                asList(LocalDate.parse("1970-01-03"), null, 1L)));

    }

    @Test
    public void testDifferentCollections() throws Exception {
        QueryResult result = getRetentionQueryExecutor().query(CONTEXT,
                Optional.of(RetentionAction.create("test0", Optional.empty())),
                Optional.of(RetentionAction.create("test1", Optional.empty())), DAY, Optional.empty(),
                Optional.of(15), LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR), ZoneOffset.UTC, false)
                .getResult().join();

        assertFalse(result.isFailed());
        assertEquals(result.getResult(), of(
                asList(LocalDate.parse("1970-01-01"), null, 1L),
                asList(LocalDate.parse("1970-01-01"), 1L, 1L),
                asList(LocalDate.parse("1970-01-02"), null, 1L),
                asList(LocalDate.parse("1970-01-03"), null, 1L)));
    }

    @Test
    public void testFilter() throws Exception {
        QueryResult result = getRetentionQueryExecutor().query(CONTEXT,
                Optional.of(RetentionAction.create("test0", Optional.of("teststr = 'test0'"))),
                Optional.of(RetentionAction.create("test1", Optional.of("teststr = 'test0'"))),
                DAY, Optional.empty(),
                Optional.of(15), LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR), ZoneOffset.UTC, false)
                .getResult().join();

        assertFalse(result.isFailed());
        assertEquals(result.getResult(), of(
                asList(LocalDate.parse("1970-01-01"), null, 1L),
                asList(LocalDate.parse("1970-01-01"), 1L, 1L),
                asList(LocalDate.parse("1970-01-03"), null, 1L)));
    }

    @Test
    public void testDimension() throws Exception {
        QueryResult result = getRetentionQueryExecutor().query(CONTEXT,
                Optional.of(RetentionAction.create("test0", Optional.empty())),
                Optional.of(RetentionAction.create("test1", Optional.empty())),
                DAY, Optional.of("teststr"), Optional.of(15),
                LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR), ZoneOffset.UTC, false).getResult().join();

        assertFalse(result.isFailed());
        assertEquals(ImmutableSet.copyOf(result.getResult()), ImmutableSet.of(
                asList("test0", null, 1L),
                asList("test0", 1L, 1L),
                asList("test1", null, 1L)));
    }

    @Test
    public void testTimeRange() throws Exception {
        QueryResult result = getRetentionQueryExecutor().query(CONTEXT,
                Optional.empty(),
                Optional.empty(),
                DAY, Optional.of("teststr"),
                Optional.of(15), LocalDate.ofEpochDay(10000), LocalDate.ofEpochDay(10000), ZoneOffset.UTC, false)
                .getResult().join();

        assertFalse(result.isFailed());
        assertEquals(result.getResult(), of());
    }

    @Test
    public void testWeeklyRetention() throws Exception {
        QueryResult result = getRetentionQueryExecutor().query(CONTEXT,
                Optional.empty(),
                Optional.empty(),
                WEEK, Optional.empty(),
                Optional.of(15), LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR), ZoneOffset.UTC, false)
                .getResult().join();

        assertFalse(result.isFailed());
        assertEquals(result.getResult(), of(
                asList(LocalDate.parse("1969-12-29"), null, 2L)));
    }

    @Test
    public void testMonthlyRetention() throws Exception {
        QueryResult result = getRetentionQueryExecutor().query(CONTEXT,
                Optional.empty(),
                Optional.empty(),
                MONTH, Optional.empty(),
                Optional.of(15), LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(SCALE_FACTOR), ZoneOffset.UTC, false)
                .getResult().join();

        assertFalse(result.isFailed());
        assertEquals(result.getResult(), of(
                asList(LocalDate.parse("1970-01-01"), null, 2L)));
    }
}
