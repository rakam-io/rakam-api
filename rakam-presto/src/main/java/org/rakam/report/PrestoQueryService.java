package org.rakam.report;

import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Statement;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.rakam.collection.event.metastore.EventSchemaMetastore;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.MaterializedViewService;
import org.rakam.util.QueryFormatter;

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

    @Override
    protected String buildQuery(String project, Statement statement) {

        StringBuilder builder = new StringBuilder(project);
        // TODO: does cold storage supports schemas?
        new QueryFormatter(builder, node -> {
            QualifiedName prefix = node.getName().getPrefix().orElse(new QualifiedName(prestoConfig.getColdStorageConnector()));
            return prefix.getSuffix() + "." + project + "." + node.getName().getSuffix();
        }).process(statement, 0);

        return builder.toString();
    }

}
