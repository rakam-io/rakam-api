package org.rakam.analysis;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.rakam.analysis.metadata.QueryMetadataStore;
import org.rakam.plugin.MaterializedView;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public abstract class TestQueryMetastore {
    private static final String PROJECT_NAME = TestQueryMetastore.class.getName().replace(".", "_").toLowerCase();


    public abstract QueryMetadataStore getQuerymetastore();

    @Test
    public void testMaterializedViewCreateGetList()
            throws Exception {
        MaterializedView materializedView = new MaterializedView("test", "test",
                "select 1", null, false, false, ImmutableMap.of());
        getQuerymetastore().createMaterializedView(PROJECT_NAME, materializedView);

        assertEquals(getQuerymetastore().getMaterializedView(PROJECT_NAME, "test"),
                materializedView);

        assertEquals(getQuerymetastore().getMaterializedViews(PROJECT_NAME),
                ImmutableList.of(materializedView));
    }
}
