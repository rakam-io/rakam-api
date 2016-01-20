package org.rakam.analysis;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.ContinuousQueryService;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public abstract class TestContinuousQueryService {
    @Test
    public void testCreate1() throws Exception {
        ContinuousQuery report = new ContinuousQuery("test", "test",
                "test", "select count(*) as count from test",
                ImmutableList.of(), ImmutableMap.of());
        getContinuousQueryService().create(report).join();

        assertEquals(getContinuousQueryService().get("test", "test"), report);
    }
    @Test
    public void testCreate() throws Exception {
        ContinuousQuery report = new ContinuousQuery("test", "test",
                "test", "select count(*) as count from test",
                ImmutableList.of(), ImmutableMap.of());
        getContinuousQueryService().create(report).join();

        assertEquals(getContinuousQueryService().getSchemas("test"),
                ImmutableMap.of("test", ImmutableList.of(new SchemaField("count", FieldType.LONG))));
    }

    public abstract ContinuousQueryService getContinuousQueryService();
}
