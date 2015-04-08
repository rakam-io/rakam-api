package org.rakam;

import com.facebook.presto.sql.ExpressionFormatter;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.impossibl.postgres.api.jdbc.PGConnection;
import com.impossibl.postgres.api.jdbc.PGNotificationListener;
import org.rakam.plugin.CollectionStreamQuery;
import org.rakam.plugin.EventStream;
import org.rakam.plugin.StreamResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 06/04/15 23:50.
 */
public class PostgresqlEventStreamer implements EventStream.EventStreamer {
    final static Logger LOGGER = LoggerFactory.getLogger(PostgresqlEventStream.class);

    private final PGConnection conn;
    private final String ticket;
    private final StreamResponse response;
    private final List<CollectionStreamQuery> collections;
    private final String project;
    PGNotificationListener listener;
    private Queue<String> queue = new ConcurrentLinkedQueue<>();

    public PostgresqlEventStreamer(PGConnection conn, String project, List<CollectionStreamQuery> collections, StreamResponse response) {
        this.conn = conn;
        this.ticket = UUID.randomUUID().toString().substring(0, 8);
        this.response = response;
        this.collections = collections;
        this.project = project;

        createProcedures();
        listener = (processId, channelName, payload) -> {
            // the payload is the json string that contains event attributes
            queue.add(payload);
        };
        conn.addNotificationListener(listener);

        try {
            Statement statement = conn.createStatement();
            statement.execute("LISTEN " + ticket);
            conn.commit();
        } catch (SQLException e) {
            Throwables.propagate(e);
        }
    }

    @Override
    public void sync() {
        response.send("data", "["+queue.stream().collect(Collectors.joining(", "))+"]");
    }

    @Override
    public void shutdown() {
        conn.removeNotificationListener(listener);

        try (Statement statement = conn.createStatement()) {
            statement.execute("UNLISTEN "+ticket);
            for (CollectionStreamQuery collection : collections) {
                statement.execute(format("DROP TRIGGER stream_%1$s_%2$s_%3$s ON %1$s.%2$s", project, collection.collection, ticket));
                statement.execute(format("DROP FUNCTION stream_%s_%s_%s", project, collection.collection, ticket));
            }
            conn.commit();
            conn.close();
        } catch (SQLException e) {
            LOGGER.error("Couldn't deleted functions and triggers from Postgresql server. Ticket: " + ticket, e);
        }
    }

    private boolean createProcedures() {
        for (CollectionStreamQuery collection : collections) {
            try (Statement statement = conn.createStatement()) {
                String name = format("stream_%s_%s", collection, ticket);

                statement.execute(format("CREATE OR REPLACE FUNCTION %s()" +
                                "  RETURNS trigger AS" +
                                "  $BODY$" +
                                "    BEGIN" +
                                "       IF %s THEN" +
                                "           PERFORM pg_notify('%s', row_to_json((NEW))::text);" +
                                "       END IF;" +
                                "        RETURN NEW;" +
                                "    END;" +
                                "  $BODY$ LANGUAGE plpgsql;",
                        name, createSqlExpression(collection), ticket));

                statement.execute(format("CREATE TRIGGER %s" +
                        "  AFTER INSERT" +
                        "  ON %s.%s" +
                        "  FOR EACH ROW" +
                        "  EXECUTE PROCEDURE %s();", name, project, collection));

            } catch (SQLException e) {
                try {
                    conn.rollback();
                } catch (SQLException e1) {
                    LOGGER.error("Error while executing rollback on Postgresql server", e);
                    return false;
                }
            }
        }
        return true;
    }

    private String createSqlExpression(CollectionStreamQuery collection) {
        if(collection.filter!=null) {
            return collection.filter.accept(new ExpressionFormatter.Formatter() {
                @Override
                protected String visitQualifiedNameReference(QualifiedNameReference node, Void context) {
                    List<String> parts = new ArrayList<>();
                    parts.add("NEW.");
                    parts.addAll(node.getName()
                            .getParts().stream()
                            .map(part -> '"' + part + '"')
                            .collect(Collectors.toList()));
                    return Joiner.on('.').join(parts);
                }
            }, null);
        }else {
            // to small hack to simplify the code.
            return "TRUE";
        }
    }
}
