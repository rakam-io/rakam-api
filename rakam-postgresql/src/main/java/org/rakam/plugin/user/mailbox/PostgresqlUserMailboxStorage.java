package org.rakam.plugin.user.mailbox;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.html.HtmlEscapers;
import com.google.inject.Inject;
import com.impossibl.postgres.api.jdbc.PGConnection;
import com.impossibl.postgres.api.jdbc.PGNotificationListener;
import org.rakam.AsyncPostgresqlConnectionPool;
import org.rakam.report.postgresql.PostgresqlQueryExecutor;
import org.rakam.util.JsonHelper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 30/03/15 06:22.
 */
public class PostgresqlUserMailboxStorage implements UserMailboxStorage {
    private final PostgresqlQueryExecutor queryExecutor;
    private final AsyncPostgresqlConnectionPool asyncConnPool;

    @Inject
    public PostgresqlUserMailboxStorage(PostgresqlQueryExecutor queryExecutor, AsyncPostgresqlConnectionPool asyncConnPool) {
        this.queryExecutor = queryExecutor;
        this.asyncConnPool = asyncConnPool;
    }

    @Override
    public Message send(String project, Object fromUser, Object toUser, Integer parentId, String message, Instant date) {
        try (Connection connection = queryExecutor.getConnection()) {
            PreparedStatement ps = connection.prepareStatement("INSERT INTO " + project + "._user_mailbox (from_user, to_user, parentId, message, time) VALUES (?, ?, ?, ?, ?)",
                    Statement.RETURN_GENERATED_KEYS);
            ps.setObject(1, fromUser);
            ps.setObject(2, toUser);
            ps.setObject(3, parentId);
            String escapedMessage = HtmlEscapers.htmlEscaper().escape(message);
            ps.setString(4, escapedMessage);
            ps.setTimestamp(5, new Timestamp(date.getEpochSecond()));
            ps.executeUpdate();
            ResultSet generatedKeys = ps.getGeneratedKeys();
            generatedKeys.next();
            return new Message(generatedKeys.getInt(1), fromUser, message, parentId, false, date);
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void createProject(String projectId) {
        try (Connection connection = queryExecutor.getConnection()) {
            String tableName = format("%s._user_mailbox", projectId);
            Statement statement = connection.createStatement();
            statement.execute(format("CREATE TABLE IF NOT EXISTS %s(" +
                    "  id SERIAL," +
                    "  to_user int NOT NULL," +
                    "  from_user int NOT NULL," +
                    "  message TEXT NOT NULL," +
                    "  parentId INT," +
                    "  seen BOOL DEFAULT FALSE NOT NULL," +
                    "  time TIMESTAMPZ NOT NULL," +
                    "  PRIMARY KEY (id)" +
                    "  )", tableName));

            String functionName = projectId + "_user_mailbox_notification";
            statement.execute(format("CREATE OR REPLACE FUNCTION %s()" +
                            "  RETURNS trigger AS" +
                            "  $BODY$" +
                            "    BEGIN" +
                            "        PERFORM pg_notify('%s_' || NEW.to_user || '_user_mailbox', row_to_json((NEW))::text);" +
                            "        RETURN NEW;" +
                            "    END;" +
                            "  $BODY$ LANGUAGE plpgsql;",
                    functionName, projectId));
            statement.execute(format("CREATE OR REPLACE FUNCTION %s_user_mailbox_all_listener ()" +
                            "  RETURNS trigger AS" +
                            "  $BODY$" +
                            "    BEGIN" +
                            "        PERFORM pg_notify('%s_user_mailbox_all_listener', row_to_json((NEW))::text);" +
                            "        RETURN NEW;" +
                            "    END;" +
                            "  $BODY$ LANGUAGE plpgsql;",
                    projectId, projectId));

            statement.execute(format("CREATE TRIGGER %s" +
                    "  AFTER INSERT" +
                    "  ON %s" +
                    "  FOR EACH ROW" +
                    "  EXECUTE PROCEDURE %s();", functionName, tableName, functionName));
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public MessageListener listen(String projectId, String user, Consumer<Message> consumer) {
        try (PGConnection conn = asyncConnPool.getConnection()) {
            conn.addNotificationListener(new PGNotificationListener() {
                @Override
                public void notification(int processId, String channelName, String payload) {
                    consumer.accept(JsonHelper.read(payload, Message.class));
                }
            });

            try (Statement statement = conn.createStatement()) {
                statement.execute(format("LISTEN %s_%s_user_mailbox", projectId, user));
            } catch (SQLException e) {
                throw Throwables.propagate(e);
            }

            return new MessageListener() {
                @Override
                public void shutdown() {
                    try (Statement statement = conn.createStatement()) {
                        statement.execute(format("UNLISTEN %s_%s_user_mailbox", projectId, user));
                    } catch (SQLException e) {
                        throw Throwables.propagate(e);
                    }
                }
            };
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public MessageListener listenAllUsers(String projectId, Consumer<Message> consumer) {
        try (PGConnection conn = asyncConnPool.getConnection()) {
            conn.addNotificationListener(new PGNotificationListener() {
                @Override
                public void notification(int processId, String channelName, String payload) {
                    consumer.accept(JsonHelper.read(payload, Message.class));
                }
            });

            try (Statement statement = conn.createStatement()) {
                statement.execute(format("CREATE TRIGGER %s_user_mailbox_all_listener" +
                        "  AFTER INSERT" +
                        "  ON %s._user_mailbox" +
                        "  FOR EACH ROW" +
                        "  EXECUTE PROCEDURE %s();", projectId, projectId));

                statement.execute(format("LISTEN %s_user_mailbox_all_listener", projectId));
            } catch (SQLException e) {
                throw Throwables.propagate(e);
            }

            return new MessageListener() {
                @Override
                public void shutdown() {
                    try (Statement statement = conn.createStatement()) {
                        statement.execute(format("UNLISTEN %s_user_mailbox_all_listener", projectId));
                    } catch (SQLException e) {
                        throw Throwables.propagate(e);
                    }
                }
            };
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<Message> getConversation(String project, Object userId, Integer parentId, int limit, int offset) {
        try (Connection connection = queryExecutor.getConnection()) {
            PreparedStatement ps;
            if(parentId == null) {
                ps = connection.prepareStatement("SELECT id, from_user, message, seen, time FROM " + project +
                        "._user_mailbox WHERE to_user = ? AND parentId IS NULL ORDER BY time DESC LIMIT ? OFFSET ?");
                ps.setObject(1, userId);
                ps.setInt(2, limit);
                ps.setInt(3, offset);
            } else {
                ps = connection.prepareStatement("SELECT id, from_user, message, seen, time FROM " + project +
                        "._user_mailbox WHERE to_user = ? AND (parentId = ? OR id = ?) ORDER BY time DESC LIMIT ? OFFSET ?");
                ps.setObject(1, userId);
                ps.setInt(2, parentId);
                ps.setInt(3, parentId);
                ps.setInt(4, limit);
                ps.setInt(5, offset);
            }
            ResultSet resultSet = ps.executeQuery();
            ImmutableList.Builder<Message> builder = ImmutableList.builder();
            while(resultSet.next()) {
                builder.add(new Message(resultSet.getInt(1), resultSet.getObject(2), resultSet.getString(3), parentId,
                        resultSet.getBoolean(4), resultSet.getTimestamp(5).toInstant()));
            }
            return builder.build();
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void markMessagesAsRead(String project, Object userId, int[] messageIds) {
        try (Connection connection = queryExecutor.getConnection()) {
            PreparedStatement ps = connection.prepareStatement("UPDATE "+project+
                    "._user_mailbox SET seen = true WHERE to_user = ? and id in ?");
            ps.setObject(1, userId);
            ps.setArray(2, connection.createArrayOf("int", Arrays.stream(messageIds).mapToObj(Integer::new).toArray()));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }
}
