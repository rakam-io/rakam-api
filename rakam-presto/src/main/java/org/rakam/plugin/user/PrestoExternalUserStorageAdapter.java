package org.rakam.plugin.user;

import com.facebook.presto.sql.ExpressionFormatter;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedName;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.report.PrestoQueryExecutor;
import org.rakam.report.QueryResult;
import org.rakam.report.postgresql.PostgresqlQueryExecutor;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.rakam.realtime.AggregationType.COUNT;
import static org.rakam.util.ValidationUtil.checkCollection;

public class PrestoExternalUserStorageAdapter extends AbstractPostgresqlUserStorage {
    private final PrestoQueryExecutor executor;

    @Inject
    public PrestoExternalUserStorageAdapter(PrestoQueryExecutor executor, PostgresqlQueryExecutor queryExecutor, Metastore metastore) {
        super(queryExecutor, metastore);
        this.executor = executor;
        queryExecutor.executeRawStatement("CREATE SCHEMA IF NOT EXISTS users").getResult().join();
        metastore.getProjects().forEach(this::createProject);
    }

    @Override
    public CompletableFuture<QueryResult> filter(String project, List<String> selectColumns, Expression filterExpression, List<EventFilter> eventFilter, Sorting sortColumn, long limit, long offset) {
        if (filterExpression != null) {
            return super.filter(project, selectColumns, filterExpression, eventFilter, sortColumn, limit, offset);
        }

        String eventFilterPredicate;
        if (eventFilter != null && !eventFilter.isEmpty()) {
            eventFilterPredicate = getEventFilterPredicate(project, eventFilter).stream().collect(Collectors.joining(" AND "));
        } else {
            eventFilterPredicate = null;
        }

        return executor.executeRawQuery(String.format("select distinct _user from %s %s %s limit %d",
                executor.formatTableReference(project, QualifiedName.of("_all")),
                eventFilterPredicate == null ? "" : "WHERE " + eventFilterPredicate,
                sortColumn == null ? "" : ("ORDER BY " + sortColumn.column + " " + sortColumn.order.name()), limit)).getResult();
    }

    @Override
    public List<String> getEventFilterPredicate(String project, List<EventFilter> eventFilter) {
        List<String> filters = new ArrayList<>(2);

        for (EventFilter filter : eventFilter) {
            StringBuilder builder = new StringBuilder();

            checkCollection(filter.collection);
            if (filter.aggregation == null) {
                builder.append(format("select \"_user\" from %s",
                        executor.formatTableReference(project, QualifiedName.of(filter.collection))));
                if (filter.filterExpression != null) {
                    builder.append(" where ").append(new ExpressionFormatter.Formatter().process(filter.getExpression(), true));
                }
                builder.append(" limit 10000");
                QueryResult result = executor.executeRawQuery(builder.toString()).getResult().join();
                if (result.isFailed()) {
                    throw new RakamException(result.getError().message, HttpResponseStatus.INTERNAL_SERVER_ERROR);
                }
                String ids = result.getResult().stream()
                        .map(e -> "'" + e.get(0).toString() + "'")
                        .collect(Collectors.joining(", "));
                filters.add((format("id in (%s)", ids)));
            } else {
                builder.append(format("select \"_user\" from %s",
                        executor.formatTableReference(project, QualifiedName.of(filter.collection))));

                ArrayList<String> filterList = new ArrayList<>(3);
                if (filter.filterExpression != null) {
                    filterList.add(new ExpressionFormatter.Formatter().process(filter.getExpression(), true));
                }
                if (filter.timeframe != null) {
                    if (filter.timeframe.start != null) {
                        filterList.add(String.format("_time > cast(%s as timestamp)", filter.timeframe.start.toString()));
                    }
                    if (filter.timeframe.end != null) {
                        filterList.add(String.format("_time < cast(%s as timestamp)", filter.timeframe.end.toString()));
                    }
                }
                if (!filterList.isEmpty()) {
                    builder.append(" where ").append(filterList.stream().collect(Collectors.joining(" AND ")));
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

    @Override
    public String getUserTable(String project) {
        return "users." + project;
    }
}
