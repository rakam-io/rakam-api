package org.rakam.report;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.rakam.collection.event.metastore.EventSchemaMetastore;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.MaterializedViewService;

/**
 * Created by buremba <Burak Emre Kabakcı> on 02/04/15 05:43.
 */
@Singleton
public class PrestoQueryService extends MaterializedViewService {
    private final PrestoConfig prestoConfig;


    @Inject
    public PrestoQueryService(QueryMetadataStore database, PrestoConfig prestoConfig, QueryExecutor queryExecutor, EventSchemaMetastore metastore) {
        super(queryExecutor, database, metastore);
        this.prestoConfig = prestoConfig;
    }

}
