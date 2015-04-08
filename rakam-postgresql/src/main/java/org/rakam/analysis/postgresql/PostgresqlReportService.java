package org.rakam.analysis.postgresql;

import com.facebook.presto.sql.tree.Statement;
import com.google.inject.Inject;
import org.rakam.collection.event.metastore.ReportMetadataStore;
import org.rakam.plugin.AbstractReportService;
import org.rakam.report.QueryExecutor;
import org.rakam.util.QueryFormatter;

/**
 * Created by buremba <Burak Emre Kabakcı> on 06/04/15 02:33.
 */
public class PostgresqlReportService extends AbstractReportService {

    private final PostgresqlConfig config;

    @Inject
    public PostgresqlReportService(PostgresqlConfig config, QueryExecutor queryExecutor, ReportMetadataStore database) {
        super(queryExecutor, database);
        this.config = config;
    }

    @Override
    protected String buildQuery(String project, Statement query) {
        StringBuilder builder = new StringBuilder(project);
        new QueryFormatter(builder, node -> project + "." +
                node.getName().getSuffix()).process(query, 0);

        return builder.toString();
    }
}
