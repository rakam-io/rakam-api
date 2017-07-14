package org.rakam.postgresql.plugin.user;

import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.Event;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.config.ProjectConfig;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.user.AbstractUserService;
import org.rakam.plugin.user.ISingleUserBatchOperation;
import org.rakam.postgresql.report.PostgresqlQueryExecutor;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryResult;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
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
import static org.apache.avro.Schema.Type.LONG;
import static org.apache.avro.Schema.Type.NULL;
import static org.apache.avro.Schema.Type.STRING;
import static org.rakam.util.ValidationUtil.checkCollection;
import static org.rakam.util.ValidationUtil.checkLiteral;
import static org.rakam.util.ValidationUtil.checkProject;
import static org.rakam.util.ValidationUtil.checkTableColumn;

public class PostgresqlUserService
        extends AbstractUserService
{
    private final Metastore metastore;
    private final PostgresqlQueryExecutor executor;
    private final PostgresqlUserStorage storage;
    private final ProjectConfig projectConfig;
    private final EventStore eventStore;

    @Inject
    public PostgresqlUserService(ProjectConfig projectConfig, EventStore eventStore, PostgresqlUserStorage storage, Metastore metastore, PostgresqlQueryExecutor executor)
    {
        super(storage);
        this.storage = storage;
        this.projectConfig = projectConfig;
        this.metastore = metastore;
        this.executor = executor;
        this.eventStore = eventStore;
    }

    @Override
    public CompletableFuture<List<CollectionEvent>> getEvents(String project, String user, Optional<List<String>> properties, int limit, Instant beforeThisTime)
    {
        checkProject(project);
        checkNotNull(user);
        checkArgument(limit <= 1000, "Maximum 1000 events can be fetched at once.");
        String sqlQuery = metastore.getCollections(project).entrySet().stream()
                .filter(entry -> entry.getValue().stream().anyMatch(field -> field.getName().equals(projectConfig.getUserColumn())))
                .filter(entry -> entry.getValue().stream().anyMatch(field -> field.getName().equals(projectConfig.getTimeColumn())))
                .map(entry ->
                        format("select '%s' as collection, row_to_json(coll) json, %s from %s.%s coll where _user = '%s' %s",
                                entry.getKey(), checkTableColumn(projectConfig.getTimeColumn()), checkCollection(project), checkCollection(entry.getKey()), checkLiteral(user),
                                beforeThisTime == null ? "" : format("and %s < timestamp '%s'", checkTableColumn(projectConfig.getTimeColumn()), beforeThisTime.toString())))
                .collect(Collectors.joining(" union all "));

        if (sqlQuery.isEmpty()) {
            return CompletableFuture.completedFuture(ImmutableList.<CollectionEvent>of());
        }

        CompletableFuture<QueryResult> queryResult = executor.executeRawQuery(format("select collection, json from (%s) data order by %s desc limit %d",
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

    public static final String ANONYMOUS_ID_MAPPING = "$anonymous_id_mapping";
    protected static final Schema ANONYMOUS_USER_MAPPING_SCHEMA = Schema.createRecord(of(
            new Schema.Field("id", Schema.createUnion(of(Schema.create(NULL), Schema.create(STRING))), null, null),
            new Schema.Field("_user", Schema.createUnion(of(Schema.create(NULL), Schema.create(STRING))), null, null),
            new Schema.Field("created_at", Schema.createUnion(of(Schema.create(NULL), Schema.create(LONG))), null, null),
            new Schema.Field("merged_at", Schema.createUnion(of(Schema.create(NULL), Schema.create(LONG))), null, null)
    ));
    protected static final List<SchemaField> ANONYMOUS_USER_MAPPING = of(
            new SchemaField("id", FieldType.STRING),
            new SchemaField("_user", FieldType.STRING),
            new SchemaField("created_at", FieldType.TIMESTAMP),
            new SchemaField("merged_at", FieldType.TIMESTAMP)
    );

    @Override
    public void merge(String project, Object user, Object anonymousId, Instant createdAt, Instant mergedAt)
    {

//        GenericData.Record properties = new GenericData.Record(ANONYMOUS_USER_MAPPING_SCHEMA);
//        properties.put(0, anonymousId.toString());
//        properties.put(1, user.toString());
//        properties.put(2, createdAt.toEpochMilli());
//        properties.put(3, mergedAt.toEpochMilli());
//
//        eventStore.store(new Event(project, ANONYMOUS_ID_MAPPING, null, ANONYMOUS_USER_MAPPING, properties));
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
    public QueryExecution preCalculate(String project, PreCalculateQuery query)
    {
        // no-op
        return QueryExecution.completedQueryExecution(null, QueryResult.empty());
    }

    @Override
    public void batch(String project, List<? extends ISingleUserBatchOperation> batchUserOperations)
    {
        storage.batch(project, batchUserOperations);
    }
}
