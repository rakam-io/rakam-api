package org.rakam.event;

import com.google.common.eventbus.EventBus;
import org.rakam.analysis.metadata.AbstractMetastore;
import org.rakam.collection.FieldDependencyBuilder;
import org.rakam.collection.TestMetastore;
import org.rakam.presto.analysis.PrestoMetastore;
import org.testng.annotations.BeforeMethod;

public class TestJdbcMetastore
        extends TestMetastore
{
    private AbstractMetastore metastore;

    @BeforeMethod
    public void setUpMethod() throws Exception {
        TestingEnvironment testingEnvironment = new TestingEnvironment();
        metastore = new PrestoMetastore(testingEnvironment.getPrestoMetastore(),
                new EventBus(), new FieldDependencyBuilder().build(), testingEnvironment.getPrestoConfig());
        metastore.setup();
    }

    @Override
    public AbstractMetastore getMetastore()
    {
        return metastore;
    }
}
