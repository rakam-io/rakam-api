package org.rakam;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.impossibl.postgres.api.jdbc.PGConnection;
import org.rakam.analysis.postgresql.PostgresqlConfig;
import org.rakam.plugin.CollectionStreamQuery;
import org.rakam.plugin.EventStream;
import org.rakam.plugin.StreamResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 06/04/15 23:31.
 */
public class PostgresqlEventStream implements EventStream {
    final static Logger LOGGER = LoggerFactory.getLogger(PostgresqlEventStream.class);

    private final PostgresqlConfig config;

    @Inject
    public PostgresqlEventStream(PostgresqlConfig config) {
        this.config = config;
    }

    @Override
    public EventStreamer subscribe(String project, List<CollectionStreamQuery> collections, List<String> columns, StreamResponse response) {
        PGConnection conn = getConnection();
        return new PostgresqlEventStreamer(conn, project, collections, response);
    }

    // TODO: create pool
    private PGConnection getConnection() {
        try {
            PGConnection connection = (PGConnection) DriverManager.getConnection(
                    format("jdbc:pgsql://%s:%s/%s?user=%s&password%s", config.getHost(), 5432, config.getDatabase(),
                            config.getUsername(),
                            config.getPassword()));
            connection.setAutoCommit(false);
            return connection;
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }



}
