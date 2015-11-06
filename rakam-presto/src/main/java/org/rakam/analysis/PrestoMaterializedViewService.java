package org.rakam.analysis;

import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.MaterializedViewService;
import org.rakam.report.QueryExecutor;

import javax.inject.Inject;
import java.time.Clock;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PrestoMaterializedViewService extends MaterializedViewService {
    public final static String MATERIALIZED_VIEW_PREFIX = "_materialized_";

    private final Metastore metastore;

    @Inject
    public PrestoMaterializedViewService(QueryExecutor executor, QueryMetadataStore database, Metastore metastore, Clock clock) {
        super(executor, database, clock);
        this.metastore = metastore;
    }

    @Override
    public Map<String, List<SchemaField>> getSchemas(String project) {
        return list(project).stream()
                .map(view -> new AbstractMap.SimpleImmutableEntry<>(view.tableName, metastore.getCollection(project, MATERIALIZED_VIEW_PREFIX + view.tableName)))
                .collect(Collectors.toMap(t -> t.getKey(), t -> t.getValue()));
    }
}
