package org.rakam.postgresql.plugin.user;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.postgresql.util.PSQLException;
import org.rakam.analysis.ConfigManager;
import org.rakam.analysis.RequestContext;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.Event;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.config.ProjectConfig;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.user.AbstractUserService;
import org.rakam.plugin.user.ISingleUserBatchOperation;
import org.rakam.postgresql.PostgresqlModule;
import org.rakam.postgresql.report.PostgresqlQueryExecutor;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryResult;
import org.rakam.util.AvroUtil;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableList.of;
import static java.lang.String.format;
import static org.rakam.analysis.InternalConfig.USER_TYPE;
import static org.rakam.util.ValidationUtil.*;

public class PostgresqlUserService
        extends AbstractUserService {
    public static final String ANONYMOUS_ID_MAPPING = "$anonymous_id_mapping";
    protected static final Map<FieldType, List<SchemaField>> ANONYMOUS_USER_MAPPING = ImmutableMap.of(
            FieldType.STRING, of(
                    new SchemaField("id", FieldType.STRING),
                    new SchemaField("_user", FieldType.STRING),
                    new SchemaField("created_at", FieldType.TIMESTAMP),
                    new SchemaField("merged_at", FieldType.TIMESTAMP)),

            FieldType.LONG, of(
                    new SchemaField("id", FieldType.STRING),
                    new SchemaField("_user", FieldType.STRING),
                    new SchemaField("created_at", FieldType.TIMESTAMP),
                    new SchemaField("merged_at", FieldType.TIMESTAMP)),

            FieldType.INTEGER, of(
                    new SchemaField("id", FieldType.STRING),
                    new SchemaField("_user", FieldType.STRING),
                    new SchemaField("created_at", FieldType.TIMESTAMP),
                    new SchemaField("merged_at", FieldType.TIMESTAMP))
    );
    protected static final Map<FieldType, Schema> ANONYMOUS_USER_MAPPING_SCHEMA = ImmutableMap.of(
            FieldType.STRING, AvroUtil.convertAvroSchema(ANONYMOUS_USER_MAPPING.get(FieldType.STRING)),
            FieldType.LONG, AvroUtil.convertAvroSchema(ANONYMOUS_USER_MAPPING.get(FieldType.LONG)),
            FieldType.INTEGER, AvroUtil.convertAvroSchema(ANONYMOUS_USER_MAPPING.get(FieldType.INTEGER)));
    private final Metastore metastore;
    private final PostgresqlQueryExecutor executor;
    private final AbstractPostgresqlUserStorage storage;
    private final ProjectConfig projectConfig;
    private final EventStore eventStore;
    private final ConfigManager configManager;

    @Inject
    public PostgresqlUserService(ProjectConfig projectConfig, ConfigManager configManager, EventStore eventStore, AbstractPostgresqlUserStorage storage, Metastore metastore, PostgresqlQueryExecutor executor) {
        super(storage);
        this.storage = storage;
        this.projectConfig = projectConfig;
        this.metastore = metastore;
        this.executor = executor;
        this.eventStore = eventStore;
        this.configManager = configManager;
    }

    @Override
    public CompletableFuture<List<CollectionEvent>> getEvents(RequestContext context, String user, Optional<List<String>> properties, int limit, Instant beforeThisTime) {
        checkProject(context.project);
        checkNotNull(user);
        checkArgument(limit <= 1000, "Maximum 1000 events can be fetched at once.");
        String sqlQuery = metastore.getCollections(context.project).entrySet().stream()
                .filter(entry -> entry.getValue().stream().anyMatch(field -> field.getName().equals(projectConfig.getUserColumn())))
                .filter(entry -> entry.getValue().stream().anyMatch(field -> field.getName().equals(projectConfig.getTimeColumn())))
                .map(entry ->
                        format("select '%s' as collection, row_to_json(coll) json, %s from %s.%s coll where _user = '%s' %s",
                                entry.getKey(), checkTableColumn(projectConfig.getTimeColumn()), checkCollection(context.project), checkCollection(entry.getKey()), checkLiteral(user),
                                beforeThisTime == null ? "" : format("and %s < timestamp '%s'", checkTableColumn(projectConfig.getTimeColumn()), beforeThisTime.toString())))
                .collect(Collectors.joining(" union all "));

        if (sqlQuery.isEmpty()) {
            return CompletableFuture.completedFuture(ImmutableList.<CollectionEvent>of());
        }

        CompletableFuture<QueryResult> queryResult = executor.executeRawQuery(context, format("select collection, json from (%s) data order by %s desc limit %d",
                sqlQuery, checkTableColumn(projectConfig.getTimeColumn()), limit), ZoneOffset.UTC, ImmutableMap.of()).getResult();
        return queryResult.thenApply(result -> {
            if (result.isFailed()) {
                throw new RakamException(result.getError().toString(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
            }

            List<CollectionEvent> events = new ArrayList(result.getResult().size());
            events.addAll(result.getResult().stream()
                    .map(objects -> {
                        Map<String, Object> read = JsonHelper.read(objects.get(1).toString(), Map.class);
                        return new CollectionEvent((String) objects.get(0), read);
                    })
                    .collect(Collectors.toList()));

            return events;
        });
    }

    public void mergeInternal(String project, Object user, Object anonymousId, Instant createdAt, Instant mergedAt) {
        FieldType config = configManager.getConfig(project, USER_TYPE.name(), FieldType.class);
        GenericData.Record properties = new GenericData.Record(ANONYMOUS_USER_MAPPING_SCHEMA.get(config));
        properties.put(0, anonymousId.toString());

        try {
            if (config == FieldType.STRING) {
                properties.put(1, user.toString());
            } else if (config == FieldType.LONG) {
                properties.put(1, Long.parseLong(user.toString()));
            } else if (config == FieldType.INTEGER) {
                properties.put(1, Integer.parseInt(user.toString()));
            } else {
                throw new IllegalStateException();
            }
        } catch (NumberFormatException e) {
            throw new RakamException("User type doesn't match", HttpResponseStatus.BAD_REQUEST);
        }

        properties.put(2, createdAt.toEpochMilli());
        properties.put(3, mergedAt.toEpochMilli());
        eventStore.store(new Event(project, ANONYMOUS_ID_MAPPING, null, ANONYMOUS_USER_MAPPING.get(config), properties));
    }

    @Override
    public void merge(String project, Object user, Object anonymousId, Instant createdAt, Instant mergedAt) {
        try {
            mergeInternal(project, user, anonymousId, createdAt, mergedAt);
        } catch (RuntimeException e) {
            if (e.getCause() instanceof PSQLException && ((PSQLException) e.getCause()).getSQLState().equals("42P01")) {
                new PostgresqlModule.UserMergeTableHook(projectConfig, executor)
                        .createTable(project).join();
                mergeInternal(project, user, anonymousId, createdAt, mergedAt);
            }
        }
    }

    private void syncAnonymousUser() {
//        for (Map.Entry<String, List<SchemaField>> entry : metastore.getCollections(project).entrySet()) {
//            if (!entry.getValue().stream().anyMatch(e -> e.getName().equals("_user")) ||
//                    !entry.getValue().stream().anyMatch(e -> e.getName().equals("_device_id"))) {
//                continue;
//            }
//            try (Connection connection = executor.getConnection()) {
//                PreparedStatement ps = connection.prepareStatement(format("UPDATE %s SET _user = ? WHERE _device_id = ? AND _user is NULL AND %s BETWEEN ? and ?",
//                        executor.formatTableReference(project, QualifiedName.of(entry.getKey()), Optional.empty(), ImmutableMap.of()),
//                        checkTableColumn(projectConfig.getTimeColumn())));
//                storage.setUserId(project, ps, user, 1);
//                storage.setUserId(project, ps, anonymousId, 2);
//                ps.setTimestamp(3, Timestamp.from(createdAt));
//                ps.setTimestamp(4, Timestamp.from(mergedAt));
//                ps.executeUpdate();
//            }
//            catch (SQLException e) {
//                throw new RuntimeException(e);
//            }
//        }
    }

    @Override
    public QueryExecution preCalculate(String project, PreCalculateQuery query) {
        // no-op
        return QueryExecution.completedQueryExecution(null, QueryResult.empty());
    }

    @Override
    public CompletableFuture<Void> batch(String project, List<? extends ISingleUserBatchOperation> batchUserOperations) {
        return storage.batch(project, batchUserOperations);
    }
}
