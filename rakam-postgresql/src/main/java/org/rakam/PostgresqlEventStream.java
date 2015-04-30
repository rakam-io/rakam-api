package org.rakam;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.impossibl.postgres.api.jdbc.PGConnection;
import org.rakam.plugin.CollectionStreamQuery;
import org.rakam.plugin.EventStream;
import org.rakam.plugin.StreamResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 06/04/15 23:31.
 */
public class PostgresqlEventStream implements EventStream {
    final static Logger LOGGER = LoggerFactory.getLogger(PostgresqlEventStream.class);
    private final AsyncPostgresqlConnectionPool pool;


    @Inject
    public PostgresqlEventStream(AsyncPostgresqlConnectionPool pool) {
        this.pool = pool;
    }

    @Override
    public EventStreamer subscribe(String project, List<CollectionStreamQuery> collections, List<String> columns, StreamResponse response) {
        try(PGConnection connection = pool.getConnection()) {
            return new PostgresqlEventStreamer(connection, project, collections, response);
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }



}
