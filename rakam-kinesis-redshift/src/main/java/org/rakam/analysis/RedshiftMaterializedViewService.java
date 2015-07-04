package org.rakam.analysis;

import com.google.inject.Inject;
import org.rakam.collection.SchemaField;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.MaterializedViewService;
import org.rakam.report.QueryExecutor;

import java.time.Clock;
import java.util.List;
import java.util.Map;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/07/15 19:25.
 */
public class RedshiftMaterializedViewService extends MaterializedViewService {
    @Inject
    public RedshiftMaterializedViewService(QueryExecutor queryExecutor, QueryMetadataStore database, Clock clock) {
        super(queryExecutor, database, clock);
    }

    @Override
    public Map<String, List<SchemaField>> getSchemas(String s) {
        return null;
    }
}
