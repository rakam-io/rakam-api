package org.rakam.plugin.user.mailbox;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.html.HtmlEscapers;
import com.google.inject.Inject;
import com.impossibl.postgres.api.jdbc.PGConnection;
import com.impossibl.postgres.api.jdbc.PGNotificationListener;
import com.impossibl.postgres.jdbc.PGDataSource;
import org.rakam.analysis.postgresql.PostgresqlConfig;
import org.rakam.report.postgresql.PostgresqlQueryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 30/03/15 06:22.
 */
public class PostgresqlUserMailboxStorage implements UserMailboxStorage {
    final static Logger LOGGER = LoggerFactory.getLogger(PostgresqlUserMailboxStorage.class);

    private final PostgresqlQueryExecutor queryExecutor;
    private final PGConnection asyncConn;
    private final static String USER_NOTIFICATION_SUFFIX = "_user_mailbox";
    private final static String USER_NOTIFICATION_ALL_SUFFIX = "_user_mailbox_all_listener";

    @Inject
    public PostgresqlUserMailboxStorage(PostgresqlQueryExecutor queryExecutor, PostgresqlConfig config) {
        this.queryExecutor = queryExecutor;

        // TODO: make pool of connections
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
    public Message send(String project, Object fromUser, Object toUser, Integer parentId, String message, Instant date) {
        checkNotNull(fromUser, "fromUser is null");
        checkNotNull(toUser, "toUser is null");
        checkNotNull(message, "message is null");
        checkNotNull(date, "date is null");
        try (Connection connection = queryExecutor.getConnection()) {
            PreparedStatement ps = connection.prepareStatement("INSERT INTO " + project + "._user_mailbox (from_user, to_user, parentId, content, time) VALUES (?, ?, ?, ?, ?)",
                    Statement.RETURN_GENERATED_KEYS);
            ps.setObject(1, fromUser);
            ps.setObject(2, toUser);
            ps.setObject(3, parentId);
            String escapedMessage = HtmlEscapers.htmlEscaper().escape(message);
            ps.setString(4, escapedMessage);
            ps.setTimestamp(5, Timestamp.from(date));
            ps.executeUpdate();
            ResultSet generatedKeys = ps.getGeneratedKeys();
            generatedKeys.next();
            return new Message(project, generatedKeys.getInt(1), fromUser, toUser, message, parentId, false, date);
        } catch (SQLException e) {
            LOGGER.error("Error while saving user message", e);
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
                    "  content TEXT NOT NULL," +
                    "  parentId INT," +
                    "  seen BOOL DEFAULT FALSE NOT NULL," +
                    "  time TIMESTAMPZ NOT NULL," +
                    "  PRIMARY KEY (id)" +
                    "  )", tableName));

            statement.execute(format("CREATE OR REPLACE FUNCTION user_mailbox_notification()" +
                    "  RETURNS trigger AS" +
                    "  $BODY$" +
                    "    BEGIN" +
                    "        PERFORM pg_notify('%1$s_' || NEW.to_user || '" + USER_NOTIFICATION_SUFFIX + "', 'msg\n' || row_to_json((NEW))::text);" +
                    "        PERFORM pg_notify('%1$s_' || NEW.from_user || '" + USER_NOTIFICATION_SUFFIX + "', 'msg\n' || row_to_json((NEW))::text);" +
                    "        PERFORM pg_notify('%1$s" + USER_NOTIFICATION_ALL_SUFFIX + "', 'msg\n' || row_to_json((NEW))::text);" +
                    "        RETURN NEW;" +
                    "    END;" +
                    "  $BODY$ LANGUAGE plpgsql;", projectId));

            statement.execute(format("DROP TRIGGER IF EXISTS user_mailbox_notification ON %s", tableName));
            statement.execute(format("CREATE TRIGGER user_mailbox_notification" +
                    "  AFTER INSERT" +
                    "  ON %s" +
                    "  FOR EACH ROW" +
                    "  EXECUTE PROCEDURE user_mailbox_notification();", tableName));
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public MessageListener listen(String projectId, String user, Consumer<Data> consumer) {
        String name = projectId + "_" + user + USER_NOTIFICATION_SUFFIX;

        PGNotificationListener listener = (processId, channelName, payload) -> {
            int idx = payload.indexOf("\n");
            Operation op = Operation.valueOf(payload.substring(0, idx));
            consumer.accept(new Data(op, payload.substring(idx + 1)));
        };
        asyncConn.addNotificationListener(name, listener);

        try (Statement statement = asyncConn.createStatement()) {
            statement.execute("LISTEN " + name);
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }

        return () -> {
            try (Statement statement = asyncConn.createStatement()) {
                statement.execute(format("UNLISTEN %s_%s" + USER_NOTIFICATION_SUFFIX, projectId, user));
            } catch (SQLException e) {
                throw Throwables.propagate(e);
            }
            asyncConn.removeNotificationListener(listener);
        };

    }

    @Override
    public MessageListener listenAllUsers(String projectId, Consumer<Data> consumer) {
        PGNotificationListener listener = (processId, channelName, payload) -> {
            int idx = payload.indexOf("\n");
            Operation op = Operation.valueOf(payload.substring(0, idx));
            consumer.accept(new Data(op, payload.substring(idx + 1)));
        };
        asyncConn.addNotificationListener(projectId + USER_NOTIFICATION_ALL_SUFFIX, listener);

        try (Statement statement = asyncConn.createStatement()) {
            statement.execute("LISTEN " + projectId + USER_NOTIFICATION_ALL_SUFFIX);
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }

        return () -> {
            try (Statement statement = asyncConn.createStatement()) {
                statement.execute(format("UNLISTEN %s_user_mailbox_all_listener", projectId));
            } catch (SQLException e) {
                throw Throwables.propagate(e);
            }
            asyncConn.removeNotificationListener(listener);
        };
    }

    @Override
    public List<Message> getConversation(String project, Object userId, Integer parentId, int limit, int offset) {
        try (Connection connection = queryExecutor.getConnection()) {
            PreparedStatement ps;
            if (parentId == null) {
                ps = connection.prepareStatement("SELECT id, from_user, content, seen, time, to_user FROM " + project +
                        "._user_mailbox WHERE to_user = ? AND parentId IS NULL ORDER BY time DESC LIMIT ? OFFSET ?");
                ps.setObject(1, userId);
                ps.setInt(2, limit);
                ps.setInt(3, offset);
            } else {
                ps = connection.prepareStatement("SELECT id, from_user, content, seen, time, to_user FROM " + project +
                        "._user_mailbox WHERE (to_user = ? OR from_user = ?) AND (parentId = ? OR id = ?) ORDER BY time DESC LIMIT ? OFFSET ?");
                ps.setObject(1, userId);
                ps.setObject(2, userId);
                ps.setInt(3, parentId);
                ps.setInt(4, parentId);
                ps.setInt(5, limit);
                ps.setInt(6, offset);
            }
            ResultSet resultSet = ps.executeQuery();
            ImmutableList.Builder<Message> builder = ImmutableList.builder();
            while (resultSet.next()) {
                builder.add(new Message(project, resultSet.getInt(1), resultSet.getObject(2), resultSet.getObject(6),
                        resultSet.getString(3), parentId,
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
            PreparedStatement ps = connection.prepareStatement("UPDATE " + project +
                    "._user_mailbox SET seen = true WHERE to_user = ? and id in ?");
            ps.setObject(1, userId);
            ps.setArray(2, connection.createArrayOf("int", Arrays.stream(messageIds).mapToObj(Integer::new).toArray()));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }
}
