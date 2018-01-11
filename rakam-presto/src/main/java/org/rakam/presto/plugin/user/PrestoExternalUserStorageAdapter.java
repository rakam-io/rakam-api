package org.rakam.presto.plugin.user;

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableMap;
import org.rakam.analysis.ConfigManager;
import org.rakam.analysis.MaterializedViewService;
import org.rakam.analysis.RequestContext;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.SchemaField;
import org.rakam.config.ProjectConfig;
import org.rakam.plugin.MaterializedView;
import org.rakam.plugin.user.UserPluginConfig;
import org.rakam.postgresql.plugin.user.AbstractPostgresqlUserStorage;
import org.rakam.postgresql.report.PostgresqlQueryExecutor;
import org.rakam.presto.analysis.PrestoConfig;
import org.rakam.presto.analysis.PrestoQueryExecutor;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutorService;
import org.rakam.report.QueryResult;
import org.rakam.util.ProjectCollection;
import org.rakam.util.RakamException;
import org.rakam.util.ValidationUtil;

import javax.inject.Inject;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.RakamSqlFormatter.formatExpression;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.lang.String.format;
import static org.rakam.presto.analysis.PrestoQueryExecution.PRESTO_TIMESTAMP_FORMAT;
import static org.rakam.presto.analysis.PrestoUserService.ANONYMOUS_ID_MAPPING;
import static org.rakam.report.realtime.AggregationType.COUNT;
import static org.rakam.util.ValidationUtil.checkCollection;
import static org.rakam.util.ValidationUtil.checkTableColumn;

public class PrestoExternalUserStorageAdapter
        extends AbstractPostgresqlUserStorage {
    private final PrestoQueryExecutor executor;
    private final UserPluginConfig config;
    private final MaterializedViewService materializedViewService;
    private final PrestoConfig prestoConfig;
    private final Metastore metastore;
    private final QueryExecutorService executorService;
    private final ProjectConfig projectConfig;

    @Inject
    public PrestoExternalUserStorageAdapter(
            ProjectConfig projectConfig,
            MaterializedViewService materializedViewService,
            PrestoQueryExecutor executor,
            QueryExecutorService executorService,
            PrestoConfig prestoConfig,
            ConfigManager configManager,
            UserPluginConfig config,
            PostgresqlQueryExecutor queryExecutor,
            Metastore metastore) {
        super(executorService, queryExecutor, configManager);
        this.projectConfig = projectConfig;
        this.executor = executor;
        this.executorService = executorService;
        this.config = config;
        this.prestoConfig = prestoConfig;
        this.materializedViewService = materializedViewService;
        queryExecutor.executeRawStatement("CREATE SCHEMA IF NOT EXISTS users").getResult().thenAccept(result -> {
            if (result.isFailed()) {
                throw new IllegalStateException("Unable to create schema for users: " + result.getError().toString());
            }
            ;
        });
        this.metastore = metastore;
    }

    @Override
    public CompletableFuture<QueryResult> searchUsers(
            RequestContext context,
            List<String> selectColumns,
            Expression filterExpression,
            List<EventFilter> eventFilter,
            Sorting sortColumn, long limit,
            String offset) {
        if (filterExpression != null && eventFilter != null && !eventFilter.isEmpty()) {
            return super.searchUsers(context, selectColumns, filterExpression, eventFilter, sortColumn, limit, offset);
        }

        String query;
        if (eventFilter != null && !eventFilter.isEmpty()) {
            query = String.format("select distinct %s as %s from (%s) ",
                    checkTableColumn(projectConfig.getUserColumn()),
                    config.getIdentifierColumn(),
                    eventFilter.stream().map(f -> String.format(getEventFilterQuery(context.project, f), checkCollection(f.collection)))
                            .collect(Collectors.joining(" union all ")));
        } else {
            List<Map.Entry<String, List<SchemaField>>> collections = metastore.getCollections(context.project)
                    .entrySet().stream()
                    .filter(c -> c.getValue().stream().anyMatch(a -> a.getName().equals("_user")))
                    .collect(Collectors.toList());

            if (collections.isEmpty()) {
                return QueryExecution.completedQueryExecution(null, QueryResult.empty()).getResult();
            }

            query = String.format("select distinct %s as %s from (%s)",
                    checkTableColumn(projectConfig.getUserColumn()),
                    config.getIdentifierColumn(),
                    collections.stream().map(c -> String.format("select %s from %s collection where collection.%s is not null", "_user",
                            checkCollection(c.getKey()), checkTableColumn(projectConfig.getUserColumn()))).collect(Collectors.joining(" union all ")));
        }

//        if(sortColumn == null) {
//            sortColumn = new Sorting("_user", Ordering.asc);
//        }

        return executorService.executeQuery(context, query +
                (sortColumn == null ? "" : (" ORDER BY " + sortColumn.column + " " + sortColumn.order.name()))
                + " LIMIT " + limit, ZoneOffset.UTC).getResult();
    }

    @Override
    public void createSegment(RequestContext context, String name, String tableName, Expression filterExpression, List<EventFilter> eventFilter, Duration interval) {

        String query;
        if (filterExpression == null) {
            query = String.format("select distinct %s as id from (%s) t",
                    checkTableColumn(projectConfig.getUserColumn()),
                    eventFilter.stream().map(f -> String.format(getEventFilterQuery(context.project, f), f.collection)).collect(Collectors.joining(" UNION ALL ")));
        } else {

            throw new RakamException("User segment must have at least one event filter", BAD_REQUEST);
        }

        materializedViewService.create(context, new MaterializedView(tableName,
                "Users who did " + (tableName == null ? "at least one event" : tableName + " event"),
                query, interval, null, null, ImmutableMap.of()));
    }

    @Override
    public QueryExecutorService getExecutorForWithEventFilter() {
        return executorService;
    }

    @Override
    public List<String> getEventFilterPredicate(String project, List<EventFilter> eventFilter) {
        return eventFilter.stream().map(f -> String.format("id in (%s)",
                String.format(getEventFilterQuery(project, f), executor.formatTableReference(project, QualifiedName.of(f.collection), Optional.empty(), ImmutableMap.of()))))
                .collect(Collectors.toList());
    }

    public String getEventFilterQuery(String project, EventFilter filter) {
        StringBuilder builder = new StringBuilder();

        builder.append("select ")
                .append(config.getEnableUserMapping() ? "coalesce(mapping._user, collection._user, collection._device_id) as _user" : "collection._user")
                .append(" from %s collection");

        ArrayList<String> filterList = new ArrayList<>(3);
        if (filter.filterExpression != null) {
            filterList.add(formatExpression(filter.getExpression(),
                    reference -> reference.getParts().stream()
                            .map(ValidationUtil::checkLiteral).collect(Collectors.joining(".")), '"'));
        }
        if (filter.timeframe != null) {
            if (filter.timeframe.start != null) {
                filterList.add(String.format("collection.%s > cast('%s' as timestamp)",
                        checkTableColumn(projectConfig.getTimeColumn()),
                        PRESTO_TIMESTAMP_FORMAT.format(filter.timeframe.start.atZone(ZoneId.of("UTC")))));
            }
            if (filter.timeframe.end != null) {
                filterList.add(String.format("collection.%s < cast('%s' as timestamp)",
                        checkTableColumn(projectConfig.getTimeColumn()),
                        PRESTO_TIMESTAMP_FORMAT.format(filter.timeframe.end.atZone(ZoneId.of("UTC")))));
            }
        }

        if (config.getEnableUserMapping() && getHasDeviceId(project, filter.collection)) {
            // mapping.created_at <= max and mapping.merged_at > min
            builder.append(String.format(" left join %s mapping on (collection.%s is null and mapping.id = collection._device_id)",
                    checkCollection(ANONYMOUS_ID_MAPPING), checkTableColumn(projectConfig.getUserColumn())));
        }

        builder.append(" where ").append(" collection." + checkTableColumn(projectConfig.getUserColumn()) + " is not null ");

        if (!filterList.isEmpty()) {
            builder.append("and ")
                    .append(filterList.stream()
                            .collect(Collectors.joining(" AND ")));
        }

        if (filter.aggregation != null) {
            String field;
            if (filter.aggregation.type == COUNT && filter.aggregation.field == null) {
                field = "collection._user";
            } else {
                field = "collection.\"" + checkTableColumn(filter.aggregation.field, "aggregation field", '"') + "\"";
            }

            if (config.getEnableUserMapping()) {
                builder.append(" group by mapping._user, collection._user, collection._device_id");
            } else {
                builder.append(" group by collection._user");
            }
            if (filter.aggregation.minimum != null || filter.aggregation.maximum != null) {
                builder.append(" having ");
            }
            if (filter.aggregation.minimum != null) {
                builder.append(format(" %s(%s) >= %d ", filter.aggregation.type, field, filter.aggregation.minimum));
            }
            if (filter.aggregation.maximum != null) {
                if (filter.aggregation.minimum != null) {
                    builder.append(" and ");
                }
                builder.append(format(" %s(%s) < %d ", filter.aggregation.type, field, filter.aggregation.maximum));
            }
        }

        return builder.toString();
    }

    private boolean getHasDeviceId(String project, String collection) {
        return metastore.getCollection(project, collection).stream()
                .anyMatch(f -> f.getName().equals("_device_id"));
    }

    @Override
    public ProjectCollection getUserTable(String project, boolean isEventFilterActive) {
        if (isEventFilterActive) {
            return new ProjectCollection(prestoConfig.getUserConnector() + ".users", project);
        }

        return new ProjectCollection("users", project);
    }
}
