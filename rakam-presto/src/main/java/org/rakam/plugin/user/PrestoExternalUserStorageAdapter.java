package org.rakam.plugin.user;

import com.facebook.presto.sql.ExpressionFormatter;
import org.rakam.analysis.postgresql.PostgresqlMetastore;
import org.rakam.report.PrestoQueryExecutor;
import org.rakam.report.postgresql.PostgresqlQueryExecutor;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.rakam.realtime.AggregationType.COUNT;
import static org.rakam.util.ValidationUtil.checkCollection;

public class PrestoExternalUserStorageAdapter extends PostgresqlUserStorageAdapter {

    private final PrestoQueryExecutor executor;

    @Inject
    public PrestoExternalUserStorageAdapter(PrestoQueryExecutor executor, PostgresqlQueryExecutor queryExecutor, PostgresqlMetastore metastore) {
        super(queryExecutor, metastore);
        this.executor = executor;
    }

    @Override
    public List<String> getEventFilterPredicate(String project, List<EventFilter> eventFilter) {
        List<String> filters = new ArrayList<>(2);

        for (EventFilter filter : eventFilter) {
            StringBuilder builder = new StringBuilder();

            checkCollection(filter.collection);
            if (filter.aggregation == null) {
                builder.append(format("select \"_user\" from %s.%s", project, filter.collection));
                if (filter.filterExpression != null) {
                    builder.append(" where ").append(new ExpressionFormatter.Formatter().process(filter.getExpression(), null));
                }
                builder.append(" limit 10000");
                String ids = executor.executeRawQuery(builder.toString()).getResult().join().getResult().stream()
                        .map(e -> "'" + e.get(0).toString() + "'")
                        .collect(Collectors.joining(", "));
                filters.add((format("id in (%s)", ids)));
            } else {
                builder.append(format("select \"_user\" from %s.%s", project, filter.collection));
                if (filter.filterExpression != null) {
                    builder.append(" where ").append(new ExpressionFormatter.Formatter().process(filter.getExpression(), null));
                }
                String field;
                if (filter.aggregation.type == COUNT && filter.aggregation.field == null) {
                    field = "_user";
                } else {
                    field = filter.aggregation.field;
                }
                builder.append(" group by \"_user\" ");
                if (filter.aggregation.minimum != null || filter.aggregation.maximum != null) {
                    builder.append(" having ");
                }
                if (filter.aggregation.minimum != null) {
                    builder.append(format(" %s(\"%s\") >= %d ", filter.aggregation.type, field, filter.aggregation.minimum));
                }
                if (filter.aggregation.maximum != null) {
                    if (filter.aggregation.minimum != null) {
                        builder.append(" and ");
                    }
                    builder.append(format(" %s(\"%s\") < %d ", filter.aggregation.type, field, filter.aggregation.maximum));
                }
                builder.append(" limit 10000");
                String ids = executor.executeRawQuery(builder.toString()).getResult().join().getResult().stream()
                        .map(e -> "'" + e.get(0).toString() + "'")
                        .collect(Collectors.joining(", "));

                filters.add((format("id in (%s)", ids)));
            }
        }

        return filters;
    }
}
