package org.rakam.plugin.user;

import com.google.inject.Inject;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.plugin.AbstractUserService;
import org.rakam.plugin.UserStorage;
import org.rakam.report.postgresql.PostgresqlQueryExecutor;
import org.rakam.util.JsonHelper;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 29/04/15 20:58.
 */
public class PostgresqlUserService extends AbstractUserService {
    private final Metastore metastore;
    private final PostgresqlQueryExecutor executor;

    @Inject
    public PostgresqlUserService(UserStorage storage, Metastore metastore, PostgresqlQueryExecutor executor) {
        super(storage);
        this.metastore = metastore;
        this.executor = executor;
    }

    @Override
    public CompletableFuture getEvents(String project, String user) {
        String sqlQuery = metastore.getCollections(project).entrySet().stream()
                .filter(entry -> entry.getValue().stream().anyMatch(field -> field.getName().equals("user")))
                .filter(entry -> entry.getValue().stream().anyMatch(field -> field.getName().equals("time")))
                .map(entry ->
                        format("select '%s' as collection, row_to_json(coll) json, time from %s.%s coll where \"user\" = %s",
                                entry.getKey(), project, entry.getKey(), user))
                .collect(Collectors.joining(" union all "));
        return executor.executeRawQuery(format("select collection, json from (%s) data order by time desc limit %d", sqlQuery, 10)).getResult()
                .thenApply(result ->
                        result.getResult().stream()
                                .map(s -> new CollectionEvent((String) s.get(0), JsonHelper.read(s.get(1).toString(), Map.class)))
                                .collect(Collectors.toList()));
    }
}
