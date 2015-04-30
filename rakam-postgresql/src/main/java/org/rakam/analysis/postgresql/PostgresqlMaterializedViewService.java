package org.rakam.analysis.postgresql;

import com.google.inject.Inject;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.MaterializedView;
import org.rakam.plugin.MaterializedViewService;
import org.rakam.report.postgresql.PostgresqlQueryExecutor;
import org.rakam.util.Tuple;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Created by buremba <Burak Emre Kabakcı> on 25/04/15 20:29.
 * TODO: replace this with Postgresql MATERIALIZED VIEW feature
 */
public class PostgresqlMaterializedViewService extends MaterializedViewService {
    private final PostgresqlMetastore metastore;
    private final PostgresqlQueryExecutor queryExecutor;

    @Inject
    public PostgresqlMaterializedViewService(PostgresqlQueryExecutor queryExecutor, QueryMetadataStore database, PostgresqlMetastore metastore, Clock clock) {
        super(queryExecutor, database, clock);
        this.metastore = metastore;
        this.queryExecutor = queryExecutor;

        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            for (MaterializedView materializedView : database.getAllMaterializedViews()) {
                long gap = clock.millis() - materializedView.lastUpdate.toEpochMilli();
                if (gap > materializedView.updateInterval.toMillis()) {
                    update(materializedView);
                }
            }
        }, 0, 5, TimeUnit.MINUTES);
    }


    public Map<String, List<SchemaField>> getSchemas(String project) {
        return list(project).stream()
                .map(view -> new Tuple<>(view.table_name, metastore.getCollection(project, queryExecutor.MATERIALIZED_VIEW_PREFIX + view.table_name)))
                .collect(Collectors.toMap(t -> t.v1(), t -> t.v2()));
    }
}
