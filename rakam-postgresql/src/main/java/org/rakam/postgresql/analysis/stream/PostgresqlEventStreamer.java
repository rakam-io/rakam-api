package org.rakam.postgresql.analysis.stream;

import com.facebook.presto.sql.ExpressionFormatter;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.impossibl.postgres.api.jdbc.PGConnection;
import com.impossibl.postgres.api.jdbc.PGNotificationListener;
import io.airlift.log.Logger;
import org.rakam.plugin.stream.CollectionStreamQuery;
import org.rakam.plugin.stream.EventStream;
import org.rakam.plugin.stream.StreamResponse;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class PostgresqlEventStreamer implements EventStream.EventStreamer {
    final static Logger LOGGER = Logger.get(PostgresqlEventStream.class);

    private final PGConnection conn;
    private boolean open;
    private final String ticket;
    private final StreamResponse response;
    private final List<CollectionStreamQuery> collections;
    private final String project;
    private final SqlParser sqlParser;
    PGNotificationListener listener;
    private Queue<String> queue = new ConcurrentLinkedQueue<>();

    public PostgresqlEventStreamer(PGConnection conn, String project, List<CollectionStreamQuery> collections, StreamResponse response) {
        this.conn = conn;
        this.ticket = "rakam_stream_"+UUID.randomUUID().toString().substring(0, 8);
        this.response = response;
        this.collections = collections;
        this.project = project;
        this.open = true;
        this.sqlParser = new SqlParser();

        if(createProcedures()) {
            listener = (processId, channelName, payload) -> {
                // the payload is the json string that contains event attributes
                queue.add(payload);
            };
            conn.addNotificationListener(ticket, listener);

            try (Statement statement = conn.createStatement()) {
                statement.execute("LISTEN " + ticket);
            } catch (SQLException e) {
                Throwables.propagate(e);
            }
        }
    }

    @Override
    public void sync() {
        if(!open) {
            response.send("error", "stream is closed").end();
        } else {
            StringBuilder builder = new StringBuilder("[");
            if(!queue.isEmpty()) {
                builder.append(queue.poll());
                for (int i = 1; i < queue.size(); i++) {
                    builder.append(", " + queue.poll());
                }
            }
            builder.append(']');
            response.send("data", builder.toString());
        }
    }

    @Override
    public synchronized void shutdown() {
        if(!open) {
            return;
        }
        conn.removeNotificationListener(listener);

        try (Statement statement = conn.createStatement()) {
            statement.execute("UNLISTEN "+ticket);
            for (CollectionStreamQuery collection : collections) {
                statement.execute(format("DROP TRIGGER IF EXISTS %s ON %1$s.%2$s",
                        getProcedureName(collection.getCollection()),
                        project,
                        collection.getCollection()));
                statement.execute(format("DROP FUNCTION IF EXISTS stream_%s_%s_%s",
                        getProcedureName(collection.getCollection())));
            }
        } catch (SQLException e) {
            LOGGER.error(e, "Couldn't deleted functions and triggers from Postgresql server. Ticket: " + ticket);
        } finally {
            open = false;
        }
    }

    private String getProcedureName(String collection) {
        return format("stream_%s_%s_%s", project, collection, ticket);
    }

    private boolean createProcedures() {
        for (CollectionStreamQuery collection : collections) {
            try (Statement statement = conn.createStatement()) {
                String name = getProcedureName(collection.getCollection());
                statement.execute(format("CREATE OR REPLACE FUNCTION %s()" +
                                "  RETURNS trigger AS" +
                                "  $BODY$" +
                                "    BEGIN" +
                                "       IF %s THEN" +
                                "           PERFORM pg_notify('%s', '{\"collection\":\"%s\", \"properties\": {' || ltrim(row_to_json((NEW))::text, '{') || '}');" +
                                "       END IF;" +
                                "        RETURN NEW;" +
                                "    END;" +
                                "  $BODY$ LANGUAGE plpgsql;",
                        name, createSqlExpression(collection), ticket, collection.getCollection()));

                statement.execute(format("CREATE TRIGGER %s" +
                        "  AFTER INSERT" +
                        "  ON \"%s\".\"%s\"" +
                        "  FOR EACH ROW" +
                        "  EXECUTE PROCEDURE %s();", name, project, collection.getCollection(), name));

            } catch (SQLException e) {
                try {
                    conn.rollback();
                    shutdown();
                } catch (SQLException e1) {
                    LOGGER.error(e, "Error while executing rollback on Postgresql server");
                    return false;
                }
            }
        }
        return true;
    }

    private String createSqlExpression(CollectionStreamQuery collection) {
        if(collection.getFilter()!=null) {
            return sqlParser.createExpression(collection.getFilter()).accept(new ExpressionFormatter.Formatter() {
                @Override
                protected String visitQualifiedNameReference(QualifiedNameReference node, Boolean context) {
                    List<String> parts = new ArrayList<>();
                    parts.add("NEW.");
                    parts.addAll(node.getName()
                            .getParts().stream()
                            .map(part -> '"' + part + '"')
                            .collect(Collectors.toList()));
                    return Joiner.on('.').join(parts);
                }
            }, true);
        }else {
            // to small hack to simplify the code.
            return "TRUE";
        }
    }
}
