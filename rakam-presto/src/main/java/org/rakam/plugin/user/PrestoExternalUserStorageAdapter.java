package org.rakam.plugin.user;

import com.facebook.presto.sql.ExpressionFormatter;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedName;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.plugin.MaterializedView;
import org.rakam.plugin.MaterializedViewService;
import org.rakam.plugin.UserPluginConfig;
import org.rakam.report.PrestoQueryExecutor;
import org.rakam.report.QueryResult;
import org.rakam.report.postgresql.PostgresqlQueryExecutor;

import javax.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.rakam.realtime.AggregationType.COUNT;
import static org.rakam.util.ValidationUtil.checkCollection;

public class PrestoExternalUserStorageAdapter extends AbstractPostgresqlUserStorage {
    private final PrestoQueryExecutor executor;
    private final UserPluginConfig config;
    private final MaterializedViewService materializedViewService;

    @Inject
    public PrestoExternalUserStorageAdapter(MaterializedViewService materializedViewService,
                                            PrestoQueryExecutor executor,
                                            UserPluginConfig config,
                                            PostgresqlQueryExecutor queryExecutor,
                                            Metastore metastore) {
        super(queryExecutor);
        this.executor = executor;
        this.config = config;
        this.materializedViewService = materializedViewService;
        queryExecutor.executeRawStatement("CREATE SCHEMA IF NOT EXISTS users").getResult().join();
        metastore.getProjects().forEach(this::createProject);
    }

    @Override
    public CompletableFuture<QueryResult> filter(String project, List<String> selectColumns, Expression filterExpression, List<EventFilter> eventFilter, Sorting sortColumn, long limit, long offset) {
        if (filterExpression != null) {
            return super.filter(project, selectColumns, filterExpression, eventFilter, sortColumn, limit, offset);
        }

        String query;
        if (eventFilter != null && !eventFilter.isEmpty()) {
            query = String.format("select distinct _user as %s from (%s)",
                    config.getIdentifierColumn(),
                    eventFilter.stream().map(f -> String.format(getEventFilterQuery(f), executor.formatTableReference(project, QualifiedName.of(f.collection))))
                            .collect(Collectors.joining(" union all ")));
        } else {
            query = String.format("select distinct _user as %s from %s",
                    config.getIdentifierColumn(),
                    executor.formatTableReference(project, QualifiedName.of("_all")));
        }

        return executor.executeRawQuery(query + " " +
                (sortColumn == null ? "" : ("ORDER BY " + sortColumn.column + " " + sortColumn.order.name())) +
                " LIMIT " + limit).getResult();
    }

    @Override
    public void createSegment(String project, String name, String tableName, Expression filterExpression, List<EventFilter> eventFilter, Duration interval) {

        String query;
        if(filterExpression == null) {
            query = String.format("select distinct _user as id from (%s)", eventFilter.stream().map(f -> String.format(getEventFilterQuery(f), f.collection)).collect(Collectors.joining(" UNION ALL ")));
        } else {
            throw new UnsupportedOperationException();
        }

        materializedViewService.create(new MaterializedView(project, name, tableName, query, interval, null));
    }

    @Override
    public List<String> getEventFilterPredicate(String project, List<EventFilter> eventFilter) {
        return eventFilter.stream().map(f -> {
            String ids = executor.executeRawQuery(String.format(getEventFilterQuery(f), executor.formatTableReference(project, QualifiedName.of(f.collection))))
                    .getResult().join().getResult().stream()
                    .map(e -> "'" + e.get(0).toString() + "'")
                    .collect(Collectors.joining(", "));

            return format("id in (%s)", ids);
        }).collect(Collectors.toList());
    }

    public String getEventFilterQuery(EventFilter filter) {
            StringBuilder builder = new StringBuilder();

            checkCollection(filter.collection);

            builder.append("select \"_user\" from %s");

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

            if (filter.aggregation != null) {
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
            }

            return builder.toString();
    }

    @Override
    public String getUserTable(String project) {
        return "users." + project;
    }
}
