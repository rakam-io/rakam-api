package org.rakam.analysis;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.rakam.EventBuilder;
import org.rakam.analysis.FunnelQueryExecutor.FunnelStep;
import org.rakam.analysis.FunnelQueryExecutor.FunnelWindow;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.Event;
import org.rakam.plugin.EventStore;
import org.rakam.report.QueryResult;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.of;
import static java.time.ZoneOffset.UTC;
import static org.rakam.analysis.FunnelQueryExecutor.FunnelType.*;
import static org.rakam.analysis.FunnelQueryExecutor.WindowType.DAY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public abstract class TestFunnelQueryExecutor {
    private static final int SCALE_FACTOR = 10;
    private static final String PROJECT_NAME = TestFunnelQueryExecutor.class.getName().replace(".", "_").toLowerCase();

    @DataProvider(name = "types")
    public static Object[][] hashEnabledValuesProvider() {
        return new Object[][]{
                {ORDERED},
                {NORMAL}
        };
    }

    @BeforeSuite
    public void setup() throws Exception {
        EventBuilder builder = new EventBuilder(PROJECT_NAME, getMetastore());

        getMetastore().createProject(PROJECT_NAME);
        for (int cIdx = 0; cIdx < 4; cIdx++) {
            final int finalCIdx = cIdx;
            List<Event> events = IntStream.range(0, SCALE_FACTOR).mapToObj(i -> builder.createEvent("test" + finalCIdx,
                    ImmutableMap.<String, Object>builder()
                            .put("teststr", "test" + (i % 2))
                            .put("_user", "test" + (i % 3))
                            .put("_time", Instant.ofEpochSecond((i * 100) + finalCIdx)).build())).collect(Collectors.toList());

            getEventStore().storeBatch(events);
        }
    }

    @AfterSuite
    public void destroy()
            throws InterruptedException {
        getMetastore().deleteProject(PROJECT_NAME);
    }

    public abstract EventStore getEventStore();

    public abstract Metastore getMetastore();

    public abstract FunnelQueryExecutor getFunnelQueryExecutor();

    @Test(dataProvider = "types")
    public void testSingleStep(FunnelQueryExecutor.FunnelType funnelType) throws Exception {
        QueryResult query = getFunnelQueryExecutor().query(new RequestContext(PROJECT_NAME, null), of(new FunnelStep("test0", null)),
                Optional.empty(),
                Optional.empty(),
                LocalDate.ofEpochDay(0),
                LocalDate.ofEpochDay(SCALE_FACTOR), Optional.of(new FunnelWindow(30, DAY)), UTC,
                Optional.empty(), funnelType).getResult().join();

        assertFalse(query.isFailed());
        assertEquals(query.getResult(), of(of("Step 1", 3L)));
    }

    @Test
    public void testSingleStepApproximate() throws Exception {
        QueryResult query = getFunnelQueryExecutor().query(new RequestContext(PROJECT_NAME, null), of(new FunnelStep("test0", null)),
                Optional.empty(),
                Optional.empty(),
                LocalDate.ofEpochDay(0),
                LocalDate.ofEpochDay(SCALE_FACTOR), Optional.empty(), UTC,
                Optional.empty(), FunnelQueryExecutor.FunnelType.APPROXIMATE).getResult().join();

        assertFalse(query.isFailed());
    }

    @Test(dataProvider = "types")
    public void testMultipleSteps(FunnelQueryExecutor.FunnelType funnelType) throws Exception {
        QueryResult query = getFunnelQueryExecutor().query(new RequestContext(PROJECT_NAME, null),
                of(new FunnelStep("test0", null), new FunnelStep("test1", null), new FunnelStep("test2", null)),
                Optional.empty(),
                Optional.empty(),
                LocalDate.ofEpochDay(0),
                LocalDate.ofEpochDay(SCALE_FACTOR), Optional.of(new FunnelWindow(30, DAY)), UTC,
                Optional.empty(), funnelType).getResult().join();

        assertFalse(query.isFailed());
        assertEquals(query.getResult(), of(of("Step 1", 3L), of("Step 2", 3L), of("Step 3", 3L)));
    }

    @Test
    public void testMultipleStepsApproximate() throws Exception {
        QueryResult query = getFunnelQueryExecutor().query(new RequestContext(PROJECT_NAME, null),
                of(new FunnelStep("test0", null), new FunnelStep("test1", null), new FunnelStep("test2", null)),
                Optional.empty(),
                Optional.empty(),
                LocalDate.ofEpochDay(0),
                LocalDate.ofEpochDay(SCALE_FACTOR), Optional.empty(), UTC,
                Optional.empty(), APPROXIMATE).getResult().join();

        assertFalse(query.isFailed());
    }

    @Test(dataProvider = "types")
    public void testMultipleStepsGrouping(FunnelQueryExecutor.FunnelType funnelType) throws Exception {
        QueryResult query = getFunnelQueryExecutor().query(new RequestContext(PROJECT_NAME, null),
                of(new FunnelStep("test0", null), new FunnelStep("test1", null), new FunnelStep("test2", null)),
                Optional.of("teststr"),
                Optional.empty(),
                LocalDate.ofEpochDay(0),
                LocalDate.ofEpochDay(SCALE_FACTOR), Optional.of(new FunnelWindow(30, DAY)), UTC,
                Optional.empty(), funnelType).getResult().join();

        assertFalse(query.isFailed());
        assertEquals(ImmutableSet.copyOf(query.getResult()), ImmutableSet.of(
                of("Step 1", "test0", 3L), of("Step 1", "test1", 3L),
                of("Step 2", "test0", 3L), of("Step 2", "test1", 3L),
                of("Step 3", "test0", 3L), of("Step 3", "test1", 3L)));
    }

    @Test
    public void testMultipleStepsGroupingApproximate() throws Exception {
        QueryResult query = getFunnelQueryExecutor().query(new RequestContext(PROJECT_NAME, null),
                of(new FunnelStep("test0", null), new FunnelStep("test1", null), new FunnelStep("test2", null)),
                Optional.of("teststr"),
                Optional.empty(),
                LocalDate.ofEpochDay(0),
                LocalDate.ofEpochDay(SCALE_FACTOR), Optional.empty(), UTC,
                Optional.empty(), APPROXIMATE).getResult().join();

        assertFalse(query.isFailed());
    }

    @Test(dataProvider = "types")
    public void testDimension(FunnelQueryExecutor.FunnelType funnelType) throws Exception {
        QueryResult query = getFunnelQueryExecutor().query(new RequestContext(PROJECT_NAME, null),
                of(new FunnelStep("test0", null), new FunnelStep("test1", null)),
                Optional.of("teststr"),
                Optional.empty(),
                LocalDate.ofEpochDay(0),
                LocalDate.ofEpochDay(SCALE_FACTOR), Optional.of(new FunnelWindow(30, DAY)), UTC,
                Optional.empty(), funnelType).getResult().join();

        assertFalse(query.isFailed());
        assertEquals(ImmutableSet.copyOf(query.getResult()),
                ImmutableSet.of(
                        of("Step 1", "test0", 3L),
                        of("Step 1", "test1", 3L),
                        of("Step 2", "test0", 3L),
                        of("Step 2", "test1", 3L)));
    }

    @Test
    public void testDimensionApproximate() throws Exception {
        QueryResult query = getFunnelQueryExecutor().query(new RequestContext(PROJECT_NAME, null),
                of(new FunnelStep("test0", null), new FunnelStep("test1", null)),
                Optional.of("teststr"),
                Optional.empty(),
                LocalDate.ofEpochDay(0),
                LocalDate.ofEpochDay(SCALE_FACTOR), Optional.of(new FunnelWindow(30, DAY)), UTC,
                Optional.empty(), NORMAL).getResult().join();

        assertFalse(query.isFailed());
    }

    @Test(dataProvider = "types")
    public void testFilter(FunnelQueryExecutor.FunnelType funnelType) throws Exception {
        QueryResult query = getFunnelQueryExecutor().query(new RequestContext(PROJECT_NAME, null),
                of(new FunnelStep("test0", Optional.of("teststr = 'test1'")), new FunnelStep("test1", Optional.of("teststr = 'test1'"))),
                Optional.of("teststr"),
                Optional.empty(),
                LocalDate.ofEpochDay(0),
                LocalDate.ofEpochDay(SCALE_FACTOR), Optional.of(new FunnelWindow(30, DAY)), UTC,
                Optional.empty(), funnelType).getResult().join();

        assertFalse(query.isFailed());
        assertEquals(query.getResult(), of(of("Step 1", "test1", 3L), of("Step 2", "test1", 3L)));
    }

    @Test
    public void testFilterApproximate() throws Exception {
        QueryResult query = getFunnelQueryExecutor().query(new RequestContext(PROJECT_NAME, null),
                of(new FunnelStep("test0", Optional.of("teststr = 'test1'")), new FunnelStep("test1", Optional.of("teststr = 'test1'"))),
                Optional.of("teststr"),
                Optional.empty(),
                LocalDate.ofEpochDay(0),
                LocalDate.ofEpochDay(SCALE_FACTOR), Optional.empty(), UTC,
                Optional.empty(), APPROXIMATE).getResult().join();

        assertFalse(query.isFailed());
    }

    @Test
    public void testSegment() throws Exception {
        QueryResult query = getFunnelQueryExecutor().query(new RequestContext(PROJECT_NAME, null),
                of(new FunnelStep("test0", Optional.of("teststr = 'test1'")), new FunnelStep("test1", Optional.of("teststr = 'test1'"))),
                Optional.of("_time"),
                Optional.of(FunnelQueryExecutor.FunnelTimestampSegments.DAY_OF_MONTH.value()),
                LocalDate.ofEpochDay(0),
                LocalDate.ofEpochDay(SCALE_FACTOR), Optional.empty(), UTC,
                Optional.empty(), NORMAL).getResult().join();
        assertFalse(query.isFailed());
        assertEquals(query.getResult(), of(of("Step 1", "1th day", 3L), of("Step 2", "1th day", 3L)));
    }

    @Test
    public void testSegmentOrdered() throws Exception {
        QueryResult query = getFunnelQueryExecutor().query(new RequestContext(PROJECT_NAME, null),
                of(new FunnelStep("test0", Optional.of("teststr = 'test1'")), new FunnelStep("test1", Optional.of("teststr = 'test1'"))),
                Optional.of("_time"),
                Optional.of(FunnelQueryExecutor.FunnelTimestampSegments.DAY_OF_MONTH.value()),
                LocalDate.ofEpochDay(0),
                LocalDate.ofEpochDay(SCALE_FACTOR), Optional.empty(), UTC,
                Optional.empty(), ORDERED).getResult().join();
        assertFalse(query.isFailed());
        assertEquals(query.getResult(), of(of("Step 1", "1th day", 3L), of("Step 2", "1th day", 3L)));
    }

    @Test
    public void testSegmentApproximate() throws Exception {
        QueryResult query = getFunnelQueryExecutor().query(new RequestContext(PROJECT_NAME, null),
                of(new FunnelStep("test0", Optional.of("teststr = 'test1'")), new FunnelStep("test1", Optional.of("teststr = 'test1'"))),
                Optional.of("_time"),
                Optional.of(FunnelQueryExecutor.FunnelTimestampSegments.DAY_OF_MONTH.value()),
                LocalDate.ofEpochDay(0),
                LocalDate.ofEpochDay(SCALE_FACTOR), Optional.empty(), UTC,
                Optional.empty(), APPROXIMATE).getResult().join();
        assertFalse(query.isFailed());
        assertEquals(query.getResult(), of(of("Step 1", "1th day", 3L), of("Step 2", "1th day", 3L)));
    }

    @Test
    public void testApproximate() throws Exception {
        QueryResult query = getFunnelQueryExecutor().query(new RequestContext(PROJECT_NAME, null),
                of(new FunnelStep("test0", Optional.of("teststr = 'test1'")), new FunnelStep("test1", Optional.of("teststr = 'test1'"))),
                Optional.empty(),
                Optional.empty(),
                LocalDate.ofEpochDay(0),
                LocalDate.ofEpochDay(SCALE_FACTOR), Optional.empty(), UTC,
                Optional.empty(), APPROXIMATE).getResult().join();
        assertFalse(query.isFailed());
        assertEquals(query.getResult(), of(of("Step 1", 3L), of("Step 2", 3L)));
    }

    @Test
    public void testSameConnectorAndDimension() throws Exception {

    }

    @Test
    public void testLongConnector() throws Exception {

    }
}
