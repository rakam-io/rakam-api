package org.rakam.analysis.postgresql;

import com.facebook.presto.sql.tree.Statement;
import com.google.inject.Inject;
import org.rakam.collection.event.metastore.EventSchemaMetastore;
import org.rakam.collection.event.metastore.QueryMetadataStore;
import org.rakam.plugin.MaterializedViewService;
import org.rakam.report.QueryExecutor;
import org.rakam.util.QueryFormatter;

/**
 * Created by buremba <Burak Emre Kabakcı> on 06/04/15 02:33.
 */
public class PostgresqlQueryService extends MaterializedViewService {
    private final PostgresqlConfig config;

    @Inject
    public PostgresqlQueryService(PostgresqlConfig config, QueryExecutor queryExecutor, QueryMetadataStore database, EventSchemaMetastore metastore) {
        super(queryExecutor, database, metastore);
        this.config = config;
    }

    @Override
    protected String buildQuery(String project, Statement statement) {
        StringBuilder builder = new StringBuilder();
        // TODO: does cold storage supports schemas?
        new QueryFormatter(builder, node ->
            project + "." + node.getName().getSuffix()
        ).process(statement, 0);

        return builder.toString();
    }
}
