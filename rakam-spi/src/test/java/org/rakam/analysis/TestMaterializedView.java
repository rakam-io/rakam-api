package org.rakam.analysis;

import com.google.common.collect.ImmutableMap;
import org.rakam.EventBuilder;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.Event;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.MaterializedView;
import org.rakam.report.QueryExecutor;
import org.rakam.report.QueryExecutorService;
import org.rakam.report.QueryResult;
import org.rakam.util.CryptUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.time.*;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.of;
import static java.lang.String.format;
import static org.rakam.util.ValidationUtil.checkCollection;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public abstract class TestMaterializedView {
    protected static final String PROJECT_NAME = TestMaterializedView.class.getSimpleName().toLowerCase(Locale.ENGLISH);
    private static final int SCALE_FACTOR = 100;

    public abstract Metastore getMetastore();

    public abstract IncrementableClock getClock();

    public abstract MaterializedViewService getMaterializedViewService();

    public abstract QueryExecutor getQueryExecutor();

    public char getEscapeIdentifier() {
        return '"';
    }

    public QueryExecutorService getQueryService() {
        return new QueryExecutorService(getQueryExecutor(), getMetastore(), getMaterializedViewService(), getEscapeIdentifier());
    }

    public abstract EventStore getEventStore();

    @BeforeSuite
    public void setup() throws Exception {
        getMetastore().createProject(PROJECT_NAME);

        populate("test");
    }

    public void populate(String collection)
            throws Exception {
        EventBuilder builder = new EventBuilder(PROJECT_NAME, getMetastore());

        List<Event> events = IntStream.range(0, SCALE_FACTOR).mapToObj(i -> builder.createEvent(collection, ImmutableMap.<String, Object>builder()
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

    @AfterMethod
    public void tearDown() throws Exception {
        QueryResult testview;
        try {
            testview = getMaterializedViewService().delete(new RequestContext(PROJECT_NAME), "testview").join();

            if (testview.isFailed()) {
                throw new IllegalStateException(testview.getError().toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCache()
            throws Exception {
        MaterializedView view = new MaterializedView("testview", "testview", "select count(*) as count from test", Duration.ofDays(1), false, false, ImmutableMap.of());
        getMaterializedViewService().create(new RequestContext(PROJECT_NAME), view).join();

        QueryResult result = getQueryService().executeQuery(PROJECT_NAME, "select * from materialized.testview", ZoneId.systemDefault()).getResult().join();
        assertFalse(result.isFailed());
        assertEquals(1, result.getResult().size());
        assertEquals(1, result.getResult().get(0).size());
        assertEquals(SCALE_FACTOR, ((Number) result.getResult().get(0).get(0)).intValue());
    }

    @Test
    public void testCacheExpiration()
            throws Exception {
        MaterializedView view = new MaterializedView("testview", "testview", "select count(*) as count from test", Duration.ofSeconds(1), false, false, ImmutableMap.of());
        getMaterializedViewService().create(new RequestContext(PROJECT_NAME), view).join();

        QueryExecutorService queryService = getQueryService();
        QueryResult result = queryService.executeQuery(PROJECT_NAME, "select * from materialized.testview", ZoneId.systemDefault()).getResult().join();
        assertFalse(result.isFailed());

        getClock().increment(Duration.ofSeconds(2).toMillis());

        result = queryService.executeQuery(PROJECT_NAME, "select * from materialized.testview", ZoneId.systemDefault()).getResult().join();
        assertFalse(result.isFailed());
        assertEquals(1, result.getResult().size());
        assertEquals(1, result.getResult().get(0).size());
        assertEquals(SCALE_FACTOR, ((Number) result.getResult().get(0).get(0)).intValue());
    }

    @Test
    public void testCacheRenewConcurrentInSingle()
            throws Exception {
        MaterializedView view = new MaterializedView("testview", "testview", "select count(*) as count from test", Duration.ofSeconds(10), false, false, ImmutableMap.of());
        getMaterializedViewService().create(new RequestContext(PROJECT_NAME), view).join();

        QueryExecutorService queryService = getQueryService();
        QueryResult result = queryService.executeQuery(PROJECT_NAME, "select * from materialized.testview", ZoneId.systemDefault()).getResult().join();
        assertFalse(result.isFailed());

        getClock().increment(Duration.ofSeconds(10).toMillis());

        List<CompletableFuture<QueryResult>> collect = IntStream.range(0, 10)
                .mapToObj(i -> queryService.executeQuery(PROJECT_NAME, "select * from materialized.testview", ZoneId.systemDefault()).getResult())
                .collect(Collectors.toList());

        for (CompletableFuture<QueryResult> future : collect) {
            result = future.join();
            assertFalse(result.isFailed());
            assertEquals(1, result.getResult().size());
            assertEquals(1, result.getResult().get(0).size());
            assertEquals(SCALE_FACTOR, ((Number) result.getResult().get(0).get(0)).intValue());
        }
    }

    @Test
    public void testIncremental()
            throws Exception {
        MaterializedView view = new MaterializedView("testview", "testview", "select count(*) as count from test", Duration.ofDays(1), true, false, ImmutableMap.of());
        getMaterializedViewService().create(new RequestContext(PROJECT_NAME), view).join();

        QueryResult result = getQueryService().executeQuery(PROJECT_NAME, "select * from materialized.testview", ZoneId.systemDefault()).getResult().join();
        assertFalse(result.isFailed());
        assertEquals(1, result.getResult().size());
        assertEquals(1, result.getResult().get(0).size());
        assertEquals(SCALE_FACTOR, ((Number) result.getResult().get(0).get(0)).intValue());
    }

    @Test
    public void testIncrementalExpirationNoData()
            throws Exception {
        MaterializedView view = new MaterializedView("testview", "testview", "select count(*) as count from test", Duration.ofSeconds(1), true, false, ImmutableMap.of());
        getMaterializedViewService().create(new RequestContext(PROJECT_NAME), view).join();

        QueryExecutorService queryService = getQueryService();
        QueryResult result = queryService.executeQuery(PROJECT_NAME, "select * from materialized.testview", ZoneId.systemDefault()).getResult().join();
        assertFalse(result.isFailed());

        getClock().increment(Duration.ofSeconds(2).toMillis());

        result = queryService.executeQuery(PROJECT_NAME, "select * from materialized.testview", ZoneId.systemDefault()).getResult().join();
        assertFalse(result.isFailed());
        int total = 0;
        for (List<Object> objects : result.getResult()) {
            total += ((Number) objects.get(0)).intValue();
        }

        assertEquals(SCALE_FACTOR, total);
    }

    @Test(invocationCount = 5)
    public void testIncrementalExpirationWithData()
            throws Exception {
        String tableName = CryptUtil.generateRandomKey(10);
        populate(tableName);

        MaterializedView view = new MaterializedView("testview", "testview", format("select count(*) as count from %s", checkCollection(tableName)), Duration.ofSeconds(1), true, false, ImmutableMap.of());
        getMaterializedViewService().create(new RequestContext(PROJECT_NAME), view).join();

        QueryExecutorService queryService = getQueryService();
        QueryResult result = queryService.executeQuery(PROJECT_NAME, "select * from materialized.testview", ZoneId.systemDefault()).getResult().join();
        assertFalse(result.isFailed());
        assertEquals(1, result.getResult().size());

        populate(tableName);

        getClock().increment(Duration.ofSeconds(2).toMillis());

        result = queryService.executeQuery(PROJECT_NAME, "select * from materialized.testview", ZoneId.systemDefault()).getResult().join();
        assertFalse(result.isFailed());

        // the insert query was fast enough
        if(result.getResult().size() == 2) {
            assertEquals(1, result.getResult().get(0).size());
            assertEquals(1, result.getResult().get(1).size());

            assertEquals(SCALE_FACTOR, ((Number) result.getResult().get(0).get(0)).intValue());
            assertEquals(SCALE_FACTOR, ((Number) result.getResult().get(1).get(0)).intValue());
        } else {
            assertEquals(1, result.getResult().size());
            assertEquals(1, result.getResult().get(0).size());
            assertEquals(SCALE_FACTOR, ((Number) result.getResult().get(0).get(0)).intValue());

            Thread.sleep(1000);

            result = queryService.executeQuery(PROJECT_NAME, "select * from materialized.testview", ZoneId.systemDefault()).getResult().join();
            assertFalse(result.isFailed());
            assertEquals(1, result.getResult().get(0).size());
            assertEquals(1, result.getResult().get(1).size());

            assertEquals(SCALE_FACTOR, ((Number) result.getResult().get(0).get(0)).intValue());
            assertEquals(SCALE_FACTOR, ((Number) result.getResult().get(1).get(0)).intValue());
        }
    }

    @Test(invocationCount = 5)
    public void testIncrementalRealtime()
            throws Exception {
        MaterializedView view = new MaterializedView("testview", "testview", "select count(*) as count from test", Duration.ofDays(1), true, true, ImmutableMap.of());
        getMaterializedViewService().create(new RequestContext(PROJECT_NAME), view).join();

        QueryResult result = getQueryService().executeQuery(PROJECT_NAME, "select * from materialized.testview", ZoneId.systemDefault()).getResult().join();
        assertFalse(result.isFailed());
        assertEquals(1, result.getResult().size());
        assertEquals(1, result.getResult().get(0).size());
        assertEquals(SCALE_FACTOR, ((Number) result.getResult().get(0).get(0)).intValue());
    }

    @Test(invocationCount = 5)
    public void testIncrementalRealtimeExpiration()
            throws Exception {
        String tableName = CryptUtil.generateRandomKey(10);
        populate(tableName);

        MaterializedView view = new MaterializedView("testview", "testview", format("select count(*) as count from %s", checkCollection(tableName)), Duration.ofSeconds(1), true, true, ImmutableMap.of());
        getMaterializedViewService().create(new RequestContext(PROJECT_NAME), view).join();

        QueryExecutorService queryService = getQueryService();
        QueryResult result = queryService.executeQuery(PROJECT_NAME, "select * from materialized.testview", ZoneId.systemDefault()).getResult().join();
        assertFalse(result.isFailed());

        populate(tableName);

        getClock().increment(Duration.ofSeconds(2).toMillis());

        result = queryService.executeQuery(PROJECT_NAME, "select * from materialized.testview", ZoneId.systemDefault()).getResult().join();
        assertFalse(result.isFailed());
        int total = 0;
        for (List<Object> objects : result.getResult()) {
            total += ((Number) objects.get(0)).intValue();
        }

        assertEquals(SCALE_FACTOR * 2, total);
    }

    public static abstract class IncrementableClock extends Clock {
        public abstract void increment(long ms);
    }
}
