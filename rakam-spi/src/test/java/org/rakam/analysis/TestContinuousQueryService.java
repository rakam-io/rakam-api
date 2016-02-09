package org.rakam.analysis;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.plugin.ContinuousQuery;
import org.rakam.plugin.ContinuousQueryService;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public abstract class TestContinuousQueryService {

    public abstract ContinuousQueryService getContinuousQueryService();
    public abstract Metastore getMetastore();

    @BeforeMethod
    public void beforeMethod() throws Exception {
        getMetastore().createProject("test");
        getMetastore().getOrCreateCollectionFieldList("test", "test", ImmutableSet.of(new SchemaField("test", FieldType.LONG)));
    }

    @AfterMethod
    public void afterMethod() throws Exception {
        getMetastore().deleteProject("test");
        getContinuousQueryService().delete("test", "streamtest");
    }

    @Test
    public void testCreate1() {
        ContinuousQuery report = new ContinuousQuery("test", "test", "streamtest", "select count(*) as count from test",
                ImmutableList.of(), ImmutableMap.of());
        getContinuousQueryService().create(report).join();

        assertEquals(getContinuousQueryService().get("test", "streamtest"), report);
    }

    @Test
    public void testSchema() throws Exception {
        ContinuousQuery report = new ContinuousQuery("test", "test",
                "streamtest", "select count(*) as count from test",
                ImmutableList.of(), ImmutableMap.of());
        getContinuousQueryService().create(report).join();

        assertEquals(getContinuousQueryService().getSchemas("test"),
                ImmutableMap.of("streamtest", ImmutableList.of(new SchemaField("count", FieldType.LONG))));
    }
}
