import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.rakam.analysis.realtime.RealtimeService;
import org.rakam.report.realtime.RealTimeReport;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.of;
import static org.rakam.report.realtime.AggregationType.COUNT;
import static org.testng.Assert.assertEquals;

public abstract class TestRealtimeModule {

    @Test
    public void testCreate() throws Exception {
        RealtimeService service = getRealtimeService();
        RealTimeReport report = new RealTimeReport("test", of(new RealTimeReport.Measure("test", COUNT)), "test", ImmutableSet.of("testcollection"), null, null);
        service.create("test", report);

        List<RealTimeReport> list = service.list("test");
        assertEquals(1, list.size());

        assertEquals("test", list.get(0).table_name);
    }

    @Test
    public void testGet() throws Exception {
        RealtimeService service = getRealtimeService();
        RealTimeReport report = new RealTimeReport("test", of(new RealTimeReport.Measure("test", COUNT)), "test", ImmutableSet.of("testcollection"), null, null);
        service.create("test", report);

        RealtimeService.RealTimeQueryResult result = service.query("test", "test", null, new RealTimeReport.Measure("test", COUNT), ImmutableList.of(), true, null, null).join();
    }

    @Test
    public void testDelete() throws Exception {
        RealtimeService service = getRealtimeService();
        RealTimeReport report = new RealTimeReport("test", of(new RealTimeReport.Measure("test", COUNT)), "test", ImmutableSet.of("testcollection"), null, null);
        service.create("test", report);
        service.delete("test", "test");

        List<RealTimeReport> list = service.list("test");
        assertEquals(0, list.size());
    }

    public abstract RealtimeService getRealtimeService();
}
