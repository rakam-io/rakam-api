package org.rakam.clickhouse;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.avro.generic.GenericData;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.collection.Event;
import org.rakam.collection.FieldType;
import org.rakam.plugin.EventStore;
import org.rakam.plugin.user.AbstractUserService;
import org.rakam.plugin.user.UserPluginConfig;
import org.rakam.plugin.user.UserStorage;
import org.rakam.report.QueryExecution;
import org.rakam.report.QueryExecutor;
import org.rakam.report.QueryResult;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;
import org.rakam.util.ValidationUtil;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static java.lang.String.format;
import static org.rakam.collection.FieldType.BINARY;
import static org.rakam.util.ValidationUtil.checkProject;

public class ClickHouseUserService extends AbstractUserService
{
    private final QueryExecutor executor;
    private final Metastore metastore;

    @Inject
    public ClickHouseUserService(UserStorage storage, EventStore eventStore, UserPluginConfig config, QueryExecutor executor, Metastore metastore)
    {
        super(storage, config, eventStore);
        this.metastore = metastore;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<List<CollectionEvent>> getEvents(String project, String user, int limit, Instant beforeThisTime)
    {

        checkProject(project);
        checkNotNull(user);

        AtomicReference<FieldType> userType = new AtomicReference<>();
        String sqlQuery = metastore.getCollections(project).entrySet().stream()
                .filter(entry -> entry.getValue().stream().anyMatch(field -> field.getName().equals("_user")))
                .filter(entry -> entry.getValue().stream().anyMatch(field -> field.getName().equals("_time")))
                .map(entry ->
                        format("select '%s' as collection, concat('{", entry.getKey()) + entry.getValue().stream()
                                .filter(field -> {
                                    if (field.getName().equals("_user")) {
                                        userType.set(field.getType());
                                        return false;
                                    }
                                    return true;
                                })
                                // for performance reasons, restrict this.
                                .filter(field -> field.getName().equals("_session_id"))
                                .filter(field -> field.getType() != BINARY)
                                .map(field -> {
                                    if (field.getType().isNumeric()) {
                                        return format("\"%1$s\": ', COALESCE(cast(%1$s as varchar), 'null'), '", field.getName());

                                    }
                                    if (field.getType().isArray() || field.getType().isMap()) {
                                        return format("\"%1$s\": ', json_format(try_cast(%1$s as json)), '", field.getName());
                                    }
                                    return format("\"%1$s\": \"', COALESCE(replace(try_cast(%1$s as varchar), '\n', '\\n'), 'null'), '\"", field.getName());
                                })
                                .collect(Collectors.joining(", ")) +
                                format(" }') as json, _time from %s where _user = %s %s",
                                        ".\"" + project + "\"." + ValidationUtil.checkCollection(entry.getKey(), '`'),
                                        userType.get().isNumeric() ? user : "'" + user + "'",
                                        beforeThisTime == null ? "" : format("and _time < toDateTime('%s')", beforeThisTime.toString())))
                .collect(Collectors.joining(" union all "));

        if(sqlQuery.isEmpty()) {
            return CompletableFuture.completedFuture(ImmutableList.of());
        }

        return executor.executeRawQuery(format("select collection, json from (%s) order by _time desc limit %d", sqlQuery, limit))
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
    public QueryExecution preCalculate(String project, PreCalculateQuery query)
    {
        return null;
    }
}
