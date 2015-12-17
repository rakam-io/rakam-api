package org.rakam.analysis;

import org.rakam.collection.event.metastore.Metastore;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.MaterializedViewService;
import org.rakam.report.QueryExecutor;

import javax.inject.Inject;
import java.time.Clock;

public class PrestoMaterializedViewService extends MaterializedViewService {
    public final static String MATERIALIZED_VIEW_PREFIX = "_materialized_";

    private final Metastore metastore;

    @Inject
    public PrestoMaterializedViewService(QueryExecutor executor, QueryMetadataStore database, Metastore metastore, Clock clock) {
        super(executor, database, clock);
        this.metastore = metastore;
    }
}
