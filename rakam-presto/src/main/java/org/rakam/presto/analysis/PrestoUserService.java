package org.rakam.presto.analysis;

import com.google.common.collect.ImmutableList;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.rakam.analysis.RequestContext;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.Event;
import org.rakam.collection.FieldType;
import org.rakam.config.ProjectConfig;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.user.AbstractUserService;
import org.rakam.plugin.user.UserPluginConfig;
import org.rakam.plugin.user.UserStorage;
import org.rakam.report.QueryExecution;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableList.of;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_IMPLEMENTED;
import static java.lang.String.format;
import static org.apache.avro.Schema.Type.*;
import static org.rakam.collection.FieldType.BINARY;
import static org.rakam.util.ValidationUtil.*;

public class PrestoUserService
        extends AbstractUserService {
    public static final String ANONYMOUS_ID_MAPPING = "$anonymous_id_mapping";
    protected static final Schema ANONYMOUS_USER_MAPPING_SCHEMA = Schema.createRecord(of(
            new Schema.Field("id", Schema.createUnion(of(Schema.create(NULL), Schema.create(STRING))), null, null),
            new Schema.Field("_user", Schema.createUnion(of(Schema.create(NULL), Schema.create(STRING))), null, null),
            new Schema.Field("created_at", Schema.createUnion(of(Schema.create(NULL), Schema.create(LONG))), null, null),
            new Schema.Field("merged_at", Schema.createUnion(of(Schema.create(NULL), Schema.create(LONG))), null, null)
    ));

    private final Metastore metastore;
    private final PrestoConfig prestoConfig;
    private final PrestoQueryExecutor executor;
    private final UserPluginConfig config;
    private final EventStore eventStore;
    private final ProjectConfig projectConfig;

    @Inject
    public PrestoUserService(
            UserStorage storage,
            ProjectConfig projectConfig,
            EventStore eventStore, Metastore metastore,
            UserPluginConfig config,
            PrestoConfig prestoConfig,
            PrestoQueryExecutor executor) {
        super(storage);
        this.metastore = metastore;
        this.prestoConfig = prestoConfig;
        this.executor = executor;
        this.config = config;
        this.projectConfig = projectConfig;
        this.eventStore = eventStore;
    }

    @Override
    public CompletableFuture<List<CollectionEvent>> getEvents(RequestContext context, String user, Optional<List<String>> properties, int limit, Instant beforeThisTime) {
        checkProject(context.project);
        checkNotNull(user);
        checkArgument(limit <= 1000, "Maximum 1000 events can be fetched at once.");

        AtomicReference<FieldType> userType = new AtomicReference<>();
        String sqlQuery = metastore.getCollections(context.project).entrySet().stream()
                .filter(entry -> entry.getValue().stream().anyMatch(field -> field.getName().equals(projectConfig.getUserColumn())))
                .filter(entry -> entry.getValue().stream().anyMatch(field -> field.getName().equals(projectConfig.getTimeColumn())))
                .map(entry ->
                        format("select '%s' as collection, '{", entry.getKey()) + entry.getValue().stream()
                                .filter(field -> {
                                    if (field.getName().equals(projectConfig.getUserColumn())) {
                                        userType.set(field.getType());
                                        return false;
                                    }
                                    return true;
                                })
                                .filter(field -> properties.isPresent() ? properties.get().contains(field.getName())
                                        : (field.getName().equals(projectConfig.getTimeColumn()) || field.getName().equals("_session_id")))
                                .filter(field -> field.getType() != BINARY)
                                .map(field -> {
                                    if (field.getType().isNumeric()) {
                                        return format("\"%1$s\": '|| COALESCE(cast(%1$s as varchar), 'null')||'", field.getName());
                                    }
                                    if (field.getType().isArray() || field.getType().isMap()) {
                                        return format("\"%1$s\": '|| json_format(try_cast(%1$s as json)) ||'", field.getName());
                                    }
                                    return format("\"%1$s\": \"'|| COALESCE(replace(try_cast(%1$s as varchar), '\n', '\\n'), 'null')||'\"", field.getName());
                                })
                                .collect(Collectors.joining(", ")) +
                                format(" }' as json, %s from %s where %s = %s %s",
                                        checkTableColumn(projectConfig.getTimeColumn()),
                                        "\"" + prestoConfig.getColdStorageConnector() + "\"" + ".\"" + context.project + "\"." + checkCollection(entry.getKey()),
                                        checkTableColumn(projectConfig.getUserColumn()),
                                        userType.get().isNumeric() ? user : "'" + user + "'",
                                        beforeThisTime == null ? "" : format("and %s < from_iso8601_timestamp('%s')", checkTableColumn(projectConfig.getTimeColumn()), beforeThisTime.toString())))
                .collect(Collectors.joining(" union all "));

        if (sqlQuery.isEmpty()) {
            return CompletableFuture.completedFuture(ImmutableList.of());
        }

        return executor.executeRawQuery(context, format("select collection, json from (%s) order by %s desc limit %d", sqlQuery, checkTableColumn(projectConfig.getTimeColumn()), limit))
                .getResult()
                .thenApply(result -> {
                    if (result.isFailed()) {
                        throw new RakamException(result.getError().message, INTERNAL_SERVER_ERROR);
                    }
                    List<CollectionEvent> collect = (List<CollectionEvent>) result.getResult().stream()
                            .map(row -> new CollectionEvent((String) row.get(0), JsonHelper.read(row.get(1).toString(), Map.class)))
                            .collect(Collectors.toList());
                    return collect;
                });
    }

    @Override
    public QueryExecution preCalculate(String project, PreCalculateQuery query) {
        String tableName = "_users_daily" +
                Optional.ofNullable(query.collection).map(value -> "_" + value).orElse("") +
                Optional.ofNullable(query.dimension).map(value -> "_by_" + value).orElse("");

        String name = "Daily users who did " +
                Optional.ofNullable(query.collection).map(value -> " event " + value).orElse(" at least one event") +
                Optional.ofNullable(query.dimension).map(value -> " grouped by " + value).orElse("");

        String table, dateColumn;
        if (query.collection == null) {
            table = String.format("SELECT cast(%s as date) as date, %s %s FROM _all",
                    checkCollection(projectConfig.getTimeColumn()),
                    Optional.ofNullable(query.dimension).map(v -> v + ",").orElse(""), checkTableColumn(projectConfig.getUserColumn()));
            dateColumn = "date";
        } else {
            table = "\"" + query.collection + "\"";
            dateColumn = String.format("cast(%s as date)", checkCollection(projectConfig.getTimeColumn()));
        }

        String sqlQuery = String.format("SELECT %s as date, %s set(_user) _user_set FROM (%s) GROUP BY 1 %s",
                dateColumn,
                Optional.ofNullable(query.dimension).map(v -> v + " as dimension,").orElse(""), table,
                Optional.ofNullable(query.dimension).map(v -> ", 2").orElse(""));

//        ContinuousQuery continuousQuery = new ContinuousQuery(tableName, name, sqlQuery,
//                ImmutableList.of("date"), ImmutableMap.of());
//        return new DelegateQueryExecution(continuousQueryService.create(project, continuousQuery, true), result -> {
//            if (result.isFailed()) {
//                throw new RakamException("Failed to create continuous query: " + JsonHelper.encode(result.getError()), INTERNAL_SERVER_ERROR);
//            }
//            result.setProperty("preCalculated", new PreCalculatedTable(name, tableName));
//            return result;
//        });
        return null;
    }

    public void merge(String project, Object user, Object anonymousId, Instant createdAt, Instant mergedAt) {
        if (!config.getEnableUserMapping()) {
            throw new RakamException(NOT_IMPLEMENTED);
        }
        GenericData.Record properties = new GenericData.Record(ANONYMOUS_USER_MAPPING_SCHEMA);
        properties.put(0, anonymousId);
        properties.put(1, user);
        properties.put(2, createdAt.toEpochMilli());
        properties.put(3, mergedAt.toEpochMilli());

        eventStore.store(new Event(project, ANONYMOUS_ID_MAPPING, null, null, properties));
    }
}
