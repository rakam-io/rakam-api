package org.rakam;

import com.google.common.base.Throwables;
import com.google.inject.name.Named;
import com.impossibl.postgres.api.jdbc.PGConnection;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.plugin.CollectionStreamQuery;
import org.rakam.plugin.EventStream;
import org.rakam.plugin.StreamResponse;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class PostgresqlEventStream implements EventStream {
    private final JDBCPoolDataSource dataSource;
    private final PGConnection pgConnection;

    @Inject
    public PostgresqlEventStream(@Named("async-postgresql") JDBCPoolDataSource dataSource) {
        this.dataSource = dataSource;
        try {
            final Connection connection = dataSource.getConnection();
            pgConnection = connection.unwrap(PGConnection.class);
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public EventStreamer subscribe(String project, List<CollectionStreamQuery> collections, List<String> columns, StreamResponse response) {
//        try(Connection connection = dataSource.getConnection()) {
//            final PGConnection unwrap = connection.unwrap(PGConnection.class);
            return new PostgresqlEventStreamer(pgConnection, project, collections, response);
//        } catch (SQLException e) {
//            throw Throwables.propagate(e);
//        }
    }

}
