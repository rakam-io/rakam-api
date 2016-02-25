package org.rakam.analysis;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.rakam.EventBuilder;
import org.rakam.analysis.FunnelQueryExecutor.FunnelStep;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.Event;
import org.rakam.plugin.EventStore;
import org.rakam.report.QueryResult;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.of;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public abstract class TestFunnelQueryExecutor {
    private static final int SCALE_FACTOR = 10;
    private static final String PROJECT_NAME = TestFunnelQueryExecutor.class.getName().replace(".", "_").toLowerCase();

    @BeforeSuite
    public void setup() throws Exception {

        EventBuilder builder = new EventBuilder(PROJECT_NAME, getMetastore());

        getMetastore().createProject(PROJECT_NAME);
        for (int cIdx = 0; cIdx < 4; cIdx ++) {
            final int finalCIdx = cIdx;
            List<Event> events = IntStream.range(0, SCALE_FACTOR).mapToObj(i -> builder.createEvent("test" + finalCIdx,
                    ImmutableMap.<String, Object>builder()
                            .put("teststr", "test" + (i % 2))
                            .put("_user", "test" + (i % 3))
                            .put("_time", Instant.ofEpochSecond(i * 100)).build())).collect(Collectors.toList());

            getEventStore().storeBatch(events);
        }
    }

    @AfterSuite
    public void destroy() {
        getMetastore().deleteProject(PROJECT_NAME);
    }

    public abstract EventStore getEventStore();

    public abstract Metastore getMetastore();

    public abstract FunnelQueryExecutor getFunnelQueryExecutor();

    @Test
    public void testSingleStep() throws Exception {
        QueryResult query = getFunnelQueryExecutor().query(PROJECT_NAME, "_user", of(new FunnelStep("test0", null)),
                Optional.empty(),
                LocalDate.ofEpochDay(0),
                LocalDate.ofEpochDay(SCALE_FACTOR), false).getResult().join();

        assertFalse(query.isFailed());
        assertEquals(query.getResult(), of(of("Step 1", 3L)));
    }

    @Test
    public void testMultipleSteps() throws Exception {
        QueryResult query = getFunnelQueryExecutor().query(PROJECT_NAME, "_user",
                of(new FunnelStep("test0", null), new FunnelStep("test1", null), new FunnelStep("test2", null)),
                Optional.empty(),
                LocalDate.ofEpochDay(0),
                LocalDate.ofEpochDay(SCALE_FACTOR), false).getResult().join();

        assertFalse(query.isFailed());
        assertEquals(query.getResult(), of(of("Step 1", 3L), of("Step 2", 3L), of("Step 3", 3L)));
    }

    @Test
    public void testMultipleStepsGrouping() throws Exception {
        QueryResult query = getFunnelQueryExecutor().query(PROJECT_NAME, "_user",
                of(new FunnelStep("test0", null), new FunnelStep("test1", null), new FunnelStep("test2", null)),
                Optional.of("teststr"),
                LocalDate.ofEpochDay(0),
                LocalDate.ofEpochDay(SCALE_FACTOR), true).getResult().join();

        assertFalse(query.isFailed());
        assertEquals(ImmutableSet.copyOf(query.getResult()), ImmutableSet.of(
                        of("Step 1", "test0", 3L), of("Step 1", "test1", 3L),
                        of("Step 2", "test0", 3L), of("Step 2", "test1", 3L),
                        of("Step 3", "test0", 3L), of("Step 3", "test1", 3L)));
    }

    @Test
    public void testDimension() throws Exception {
        QueryResult query = getFunnelQueryExecutor().query(PROJECT_NAME, "_user",
                of(new FunnelStep("test0", null), new FunnelStep("test1", null)),
                Optional.of("teststr"),
                LocalDate.ofEpochDay(0),
                LocalDate.ofEpochDay(SCALE_FACTOR), false).getResult().join();

        assertFalse(query.isFailed());
        assertEquals(ImmutableSet.copyOf(query.getResult()),
                ImmutableSet.of(of("Step 1", "test0", 3L), of("Step 1", "test1", 3L), of("Step 2", "test0", 3L), of("Step 2", "test1", 3L)));
    }

    @Test
    public void testFilter() throws Exception {
        QueryResult query = getFunnelQueryExecutor().query(PROJECT_NAME, "_user",
                of(new FunnelStep("test0", "teststr = 'test1'"), new FunnelStep("test1", "teststr = 'test1'")),
                Optional.of("teststr"),
                LocalDate.ofEpochDay(0),
                LocalDate.ofEpochDay(SCALE_FACTOR), false).getResult().join();

        assertFalse(query.isFailed());
        assertEquals(query.getResult(), of(of("Step 1", "test1", 3L), of("Step 2", "test1", 3L)));
    }

    @Test
    public void testInterval() throws Exception {
        QueryResult query = getFunnelQueryExecutor().query(PROJECT_NAME, "_user",
                of(new FunnelStep("test0", null)),
                Optional.empty(),
                LocalDate.ofEpochDay(1000),
                LocalDate.ofEpochDay(1001), false).getResult().join();

        assertFalse(query.isFailed());
        System.out.println(query.getResult());
        assertEquals(query.getResult(), of(of("Step 1", 0L)));
    }

    @Test
    public void testSameConnectorAndDimension() throws Exception {

    }

    @Test
    public void testLongConnector() throws Exception {

    }
}
