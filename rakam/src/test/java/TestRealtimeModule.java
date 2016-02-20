import com.google.common.collect.ImmutableList;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.report.realtime.RealTimeConfig;
import org.rakam.report.realtime.AggregationType;
import org.rakam.analysis.realtime.RealTimeHttpService;
import org.rakam.analysis.realtime.RealTimeHttpService.RealTimeQueryResult;
import org.rakam.report.realtime.RealTimeReport;
import org.rakam.report.QueryExecutor;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;

public abstract class TestRealtimeModule {

    @Test
    public void testCreate() throws Exception {
        RealTimeHttpService service = new RealTimeHttpService(getContinuousQueryService(), getQueryExecutor(), new RealTimeConfig(), getTimestampToEpochFunction());
        RealTimeReport report = new RealTimeReport("test", "test", AggregationType.COUNT, "test", ImmutableList.of("testcollection"), null, null, null);
        service.create(report);

        List<ContinuousQuery> list = service.list("test");
        assertEquals(1, list.size());

        assertEquals("test", list.get(0).tableName);
        assertEquals("test", list.get(0).project);
        assertEquals(ImmutableList.of(), list.get(0).partitionKeys);
        Map<String, Object> options = list.get(0).options;

        assertEquals(true, options.get("realtime"));
        assertEquals(AggregationType.COUNT, AggregationType.valueOf(options.get("aggregation").toString()));
    }

    @Test
    public void testGet() throws Exception {
        RealTimeHttpService service = new RealTimeHttpService(getContinuousQueryService(), getQueryExecutor(), new RealTimeConfig(), getTimestampToEpochFunction());
        RealTimeReport report = new RealTimeReport("test", "test", AggregationType.COUNT, "test", ImmutableList.of("testcollection"), null, null, null);
        service.create(report);

        RealTimeQueryResult result = service.get("test", "test", null, ImmutableList.of(), true, null, null).join();
    }

    @Test
    public void testDelete() throws Exception {
        RealTimeHttpService service = new RealTimeHttpService(getContinuousQueryService(), getQueryExecutor(), new RealTimeConfig(), getTimestampToEpochFunction());
        RealTimeReport report = new RealTimeReport("test", "test", AggregationType.COUNT, "test", ImmutableList.of("testcollection"), null, null, null);
        service.create(report);
        service.delete("test", "test");

        List<ContinuousQuery> list = service.list("test");
        assertEquals(0, list.size());
    }

    public abstract ContinuousQueryService getContinuousQueryService();

    public abstract QueryExecutor getQueryExecutor();

    public abstract String getTimestampToEpochFunction();
}
