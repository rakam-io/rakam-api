package org.rakam.clickhouse;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.Event;
import org.rakam.collection.FieldType;
import org.rakam.config.ProjectConfig;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.user.AbstractUserService;
import org.rakam.plugin.user.UserPluginConfig;
import org.rakam.plugin.user.UserStorage;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;
import org.rakam.util.ValidationUtil;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableList.of;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static java.lang.String.format;
import static org.apache.avro.Schema.Type.*;
import static org.rakam.collection.FieldType.BINARY;
import static org.rakam.util.ValidationUtil.checkProject;
import static org.rakam.util.ValidationUtil.checkTableColumn;

public class ClickHouseUserService extends AbstractUserService {
    public static final String ANONYMOUS_ID_MAPPING = "$anonymous_id_mapping";
    protected static final Schema ANONYMOUS_USER_MAPPING_SCHEMA = Schema.createRecord(of(
            new Schema.Field("id", Schema.createUnion(of(Schema.create(NULL), Schema.create(STRING))), null, null),
            new Schema.Field("_user", Schema.createUnion(of(Schema.create(NULL), Schema.create(STRING))), null, null),
            new Schema.Field("created_at", Schema.createUnion(of(Schema.create(NULL), Schema.create(INT))), null, null),
            new Schema.Field("merged_at", Schema.createUnion(of(Schema.create(NULL), Schema.create(INT))), null, null)
    ));

    private final QueryExecutor executor;
    private final Metastore metastore;
    private final UserPluginConfig config;
    private final EventStore eventStore;
    private final ProjectConfig projectConfig;

    @Inject
    public ClickHouseUserService(ProjectConfig projectConfig, UserStorage storage, EventStore eventStore, UserPluginConfig config, QueryExecutor executor, Metastore metastore) {
        super(storage);
        this.metastore = metastore;
        this.executor = executor;
        this.config = config;
        this.eventStore = eventStore;
        this.projectConfig = projectConfig;
    }

    @Override
    public CompletableFuture<List<CollectionEvent>> getEvents(String project, String user, Optional<List<String>> properties, int limit, Instant beforeThisTime) {

        checkProject(project);
        checkNotNull(user);

        AtomicReference<FieldType> userType = new AtomicReference<>();
        String sqlQuery = metastore.getCollections(project).entrySet().stream()
                .filter(entry -> entry.getValue().stream().anyMatch(field -> field.getName().equals(projectConfig.getUserColumn())))
                .filter(entry -> entry.getValue().stream().anyMatch(field -> field.getName().equals(projectConfig.getTimeColumn())))
                .map(entry ->
                        format("select '%s' as collection, concat('{", entry.getKey()) + entry.getValue().stream()
                                .filter(field -> {
                                    if (field.getName().equals("_user")) {
                                        userType.set(field.getType());
                                        return false;
                                    }
                                    return true;
                                })
                                .filter(field -> !properties.isPresent() || properties.get().contains(field.getName()))
                                .filter(field -> field.getType() != BINARY)
                                .map(field -> {
                                    if (field.getType().isNumeric()) {
                                        return format("\"%1$s\": ', COALESCE(cast(%1$s as varchar), 'null'), '", field.getName());

                                    }
                                    if (field.getType().isArray() || field.getType().isMap()) {
                                        return format("\"%1$s\": ', json_format(try_cast(%1$s as json)), '", field.getName());
                                    }
                                    return format("\"%1$s\": \"', COALESCE(replaceView(try_cast(%1$s as varchar), '\n', '\\n'), 'null'), '\"", field.getName());
                                })
                                .collect(Collectors.joining(", ")) +
                                format(" }') as json, %s from %s where _user = %s %s",
                                        checkTableColumn(projectConfig.getTimeColumn()),
                                        ".\"" + project + "\"." + ValidationUtil.checkCollection(entry.getKey(), '`'),
                                        userType.get().isNumeric() ? user : "'" + user + "'",
                                        beforeThisTime == null ? "" : format("and %s < toDateTime('%s')", checkTableColumn(projectConfig.getTimeColumn()), beforeThisTime.toString())))
                .collect(Collectors.joining(" union all "));

        if (sqlQuery.isEmpty()) {
            return CompletableFuture.completedFuture(ImmutableList.of());
        }

        return executor.executeRawQuery(format("select collection, json from (%s) order by %s desc limit %d", sqlQuery, checkTableColumn(projectConfig.getTimeColumn()), limit))
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

    public void merge(String project, Object user, Object anonymousId, Instant createdAt, Instant mergedAt) {
        if (!config.getEnableUserMapping()) {
            throw new RakamException(HttpResponseStatus.NOT_IMPLEMENTED);
        }
        GenericData.Record properties = new GenericData.Record(ANONYMOUS_USER_MAPPING_SCHEMA);
        properties.put(0, anonymousId);
        properties.put(1, user);
        properties.put(2, (int) Math.floorDiv(createdAt.getEpochSecond(), 86400));
        properties.put(3, (int) Math.floorDiv(mergedAt.getEpochSecond(), 86400));

        eventStore.store(new Event(project, ANONYMOUS_ID_MAPPING, null, null, properties));
    }

    @Override
    public QueryExecution preCalculate(String project, PreCalculateQuery query) {
        return null;
    }
}
