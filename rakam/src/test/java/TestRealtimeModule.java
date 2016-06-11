import com.google.common.collect.ImmutableList;
import org.rakam.analysis.RealtimeService;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.analysis.ContinuousQueryService;
import org.rakam.report.realtime.RealTimeConfig;
import org.rakam.report.realtime.AggregationType;
import org.rakam.analysis.realtime.RealTimeHttpService;
import org.rakam.report.realtime.RealTimeReport;
import org.rakam.report.QueryExecutor;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static org.rakam.report.realtime.AggregationType.COUNT;
import static org.testng.Assert.assertEquals;

public abstract class TestRealtimeModule {

    @Test
    public void testCreate() throws Exception {
        RealtimeService service = new RealtimeService(getContinuousQueryService(), getQueryExecutor(), ImmutableList.of(COUNT), new RealTimeConfig(), getTimestampToEpochFunction());
        RealTimeReport report = new RealTimeReport("test", ImmutableList.of(new RealTimeReport.Measure("test", COUNT)), "test", ImmutableList.of("testcollection"), null, null);
        service.create("test", report);

        List<ContinuousQuery> list = service.list("test");
        assertEquals(1, list.size());

        assertEquals("test", list.get(0).tableName);
        assertEquals(ImmutableList.of(), list.get(0).partitionKeys);
        Map<String, Object> options = list.get(0).options;

        assertEquals(true, options.get("realtime"));
        assertEquals(COUNT, AggregationType.valueOf(options.get("aggregation").toString()));
    }

    @Test
    public void testGet() throws Exception {
        RealtimeService service = new RealtimeService(getContinuousQueryService(), getQueryExecutor(), ImmutableList.of(COUNT), new RealTimeConfig(), getTimestampToEpochFunction());
        RealTimeReport report = new RealTimeReport("test", ImmutableList.of(new RealTimeReport.Measure("test", COUNT)), "test", ImmutableList.of("testcollection"), null, null);
        service.create("test", report);

        RealtimeService.RealTimeQueryResult result = service.query("test", "test", null, new RealTimeReport.Measure("test", COUNT), ImmutableList.of(), true, null, null).join();
    }

    @Test
    public void testDelete() throws Exception {
        RealtimeService service = new RealtimeService(getContinuousQueryService(), getQueryExecutor(), ImmutableList.of(COUNT), new RealTimeConfig(), getTimestampToEpochFunction());
        RealTimeReport report = new RealTimeReport("test", ImmutableList.of(new RealTimeReport.Measure("test", COUNT)), "test", ImmutableList.of("testcollection"), null, null);
        service.create("test", report);
        service.delete("test", "test");

        List<ContinuousQuery> list = service.list("test");
        assertEquals(0, list.size());
    }

    public abstract ContinuousQueryService getContinuousQueryService();

    public abstract QueryExecutor getQueryExecutor();

    public abstract String getTimestampToEpochFunction();
}
