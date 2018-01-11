package org.rakam.presto.analysis;

import com.google.common.eventbus.EventBus;
import org.rakam.analysis.metadata.AbstractMetastore;
import org.rakam.collection.SchemaField;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public abstract class PrestoAbstractMetastore extends AbstractMetastore {
    public PrestoAbstractMetastore(EventBus eventBus) {
        super(eventBus);
    }

    public abstract Map<String, List<SchemaField>> getSchemas(String project, Predicate<String> filter);
}