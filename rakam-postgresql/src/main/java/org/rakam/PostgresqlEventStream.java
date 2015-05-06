package org.rakam;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.impossibl.postgres.api.jdbc.PGConnection;
import com.impossibl.postgres.jdbc.PGDataSource;
import org.rakam.analysis.postgresql.PostgresqlConfig;
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
    private final PGConnection asyncConn;

    @Inject
    public PostgresqlEventStream(PostgresqlConfig config) {
        PGDataSource pg = new PGDataSource();
        pg.setDatabase(config.getDatabase());
        pg.setHost(config.getHost());
        pg.setPassword(config.getPassword());
        pg.setUser(config.getUsername());
        try {
            this.asyncConn = (PGConnection) pg.getConnection();
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }

    }

    @Override
    public EventStreamer subscribe(String project, List<CollectionStreamQuery> collections, List<String> columns, StreamResponse response) {
        return new PostgresqlEventStreamer(asyncConn, project, collections, response);
    }



}
