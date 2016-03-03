package org.rakam.presto.analysis;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.rakam.collection.Event;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.FieldType;
import org.rakam.plugin.user.AbstractUserService;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.user.UserPluginConfig;
import org.rakam.plugin.user.UserStorage;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableList.of;
import static java.lang.String.format;
import static org.apache.avro.Schema.Type.INT;
import static org.apache.avro.Schema.Type.NULL;
import static org.apache.avro.Schema.Type.STRING;
import static org.rakam.util.ValidationUtil.checkProject;

public class PrestoUserService extends AbstractUserService {
    private static final Schema ANONYMOUS_USER_MAPPING_SCHEMA = Schema.createRecord(of(
            new Schema.Field("id", Schema.createUnion(of(Schema.create(NULL), Schema.create(STRING))), null, null),
            new Schema.Field("_user", Schema.createUnion(of(Schema.create(NULL), Schema.create(STRING))), null, null),
            new Schema.Field("created_at", Schema.createUnion(of(Schema.create(NULL), Schema.create(INT))), null, null),
            new Schema.Field("merged_at", Schema.createUnion(of(Schema.create(NULL), Schema.create(INT))), null, null)
    ));
    private final Metastore metastore;
    private final PrestoConfig prestoConfig;
    private final PrestoQueryExecutor executor;
    private final EventStore eventStore;
    private final UserPluginConfig config;

    @Inject
    public PrestoUserService(UserStorage storage, EventStore eventStore, Metastore metastore,
                             UserPluginConfig config,
                             PrestoConfig prestoConfig, PrestoQueryExecutor executor) {
        super(storage);
        this.metastore = metastore;
        this.config = config;
        this.prestoConfig = prestoConfig;
        this.executor = executor;
        this.eventStore = eventStore;
    }

    @Override
    public CompletableFuture<List<CollectionEvent>> getEvents(String project, String user, int limit, Instant beforeThisTime) {
        checkProject(project);
        checkNotNull(user);
        checkArgument(limit <= 1000, "Maximum 1000 events can be fetched at once.");
        String sqlQuery = metastore.getCollections(project).entrySet().stream()
                .filter(entry -> entry.getValue().stream().anyMatch(field -> field.getName().equals("_user")))
                .filter(entry -> entry.getValue().stream().anyMatch(field -> field.getName().equals("_time")))
                .map(entry ->
                        format("select '%s' as collection, '{", entry.getKey()) + entry.getValue().stream()
                                .filter(field -> !field.getName().equals("user"))
                                .filter(field -> field.getType() != FieldType.BINARY)
                                .map(field -> {
                                    switch (field.getType()) {
                                        case LONG:
                                        case DOUBLE:
                                        case BOOLEAN:
                                            return format("\"%1$s\": '|| COALESCE(cast(%1$s as varchar), 'null')||'", field.getName());
                                        default:
                                            if (field.getType().isArray() || field.getType().isMap()) {
                                                return format("\"%1$s\": '|| json_format(try_cast(%1$s as json)) ||'", field.getName());
                                            }
                                            return format("\"%1$s\": \"'|| COALESCE(replace(try_cast(%1$s as varchar), '\n', '\\n'), 'null')||'\"", field.getName());
                                    }
                                })
                                .collect(Collectors.joining(", ")) +
                                format(" }' as json, _time from %s where _user = '%s' %s",
                                        prestoConfig.getColdStorageConnector() + "." + project + "." + entry.getKey(),
                                        user,
                                        beforeThisTime == null ? "" : format("and _time < from_iso8601_timestamp('%s')", beforeThisTime.toString())))
                .collect(Collectors.joining(" union all "));

        return executor.executeRawQuery(format("select collection, json from (%s) order by _time desc limit %d", sqlQuery, limit))
                .getResult()
                .thenApply(result -> {
                    if (result.isFailed()) {
                        throw new RakamException(result.getError().message, HttpResponseStatus.INTERNAL_SERVER_ERROR);
                    }
                    List<CollectionEvent> collect = (List<CollectionEvent>) result.getResult().stream()
                            .map(row -> new CollectionEvent((String) row.get(0), JsonHelper.read(row.get(1).toString(), Map.class)))
                            .collect(Collectors.toList());
                    return collect;
                });
    }

    @Override
    public void merge(String project, String user, String anonymousId, Instant createdAt, Instant mergedAt) {
        if(!config.getEnableUserMapping()) {
            throw new RakamException(HttpResponseStatus.NOT_IMPLEMENTED);
        }
        GenericData.Record properties = new GenericData.Record(ANONYMOUS_USER_MAPPING_SCHEMA);
        properties.put(0, anonymousId);
        properties.put(1, user);
        properties.put(2, (int) Math.floorDiv(createdAt.getEpochSecond(), 86400));
        properties.put(3, (int) Math.floorDiv(mergedAt.getEpochSecond(), 86400));

        eventStore.store(new Event(project, "_anonymous_id_mapping", null, properties));
    }
}
