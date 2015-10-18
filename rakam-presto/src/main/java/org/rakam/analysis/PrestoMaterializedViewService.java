package org.rakam.analysis;

import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.MaterializedView;
import org.rakam.plugin.MaterializedViewService;
import org.rakam.report.PrestoQueryExecutor;

import javax.inject.Inject;
import java.time.Clock;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class PrestoMaterializedViewService extends MaterializedViewService {
    public final static String MATERIALIZED_VIEW_PREFIX = "_materialized_";

    private final Metastore metastore;

    @Inject
    public PrestoMaterializedViewService(PrestoQueryExecutor queryExecutor, QueryMetadataStore database, Metastore metastore, Clock clock) {
        super(queryExecutor, database, clock);
        this.metastore = metastore;

        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            for (MaterializedView materializedView : database.getAllMaterializedViews()) {
                long gap = clock.millis() - materializedView.lastUpdate.toEpochMilli();
                if (gap > materializedView.updateInterval.toMillis()) {
                    update(materializedView);
                }
            }
        }, 0, 5, TimeUnit.MINUTES);
    }

    @Override
    public Map<String, List<SchemaField>> getSchemas(String project) {
        return list(project).stream()
                .map(view -> new AbstractMap.SimpleImmutableEntry<>(view.table_name, metastore.getCollection(project, MATERIALIZED_VIEW_PREFIX + view.table_name)))
                .collect(Collectors.toMap(t -> t.getKey(), t -> t.getValue()));
    }
}
