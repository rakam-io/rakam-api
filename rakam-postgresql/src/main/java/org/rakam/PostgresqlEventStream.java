package org.rakam;

import com.google.common.base.Throwables;
import com.impossibl.postgres.api.jdbc.PGConnection;
import com.impossibl.postgres.jdbc.PGDataSource;
import io.airlift.log.Logger;
import org.rakam.analysis.postgresql.PostgresqlConfig;
import org.rakam.plugin.CollectionStreamQuery;
import org.rakam.plugin.EventStream;
import org.rakam.plugin.StreamResponse;

import javax.inject.Inject;
import java.sql.SQLException;
import java.util.List;

public class PostgresqlEventStream implements EventStream {
    final static Logger LOGGER = Logger.get(PostgresqlEventStream.class);
    private final PGConnection asyncConn;

    @Inject
    public PostgresqlEventStream(PostgresqlConfig config) {
        PGDataSource pg = new PGDataSource();
        pg.setDatabase(config.getDatabase());
        pg.setHost(config.getHost());
        pg.setPassword(config.getPassword());
        pg.setPort(config.getPort());
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
