package org.rakam.postgresql.plugin.user.mailbox;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.html.HtmlEscapers;
import com.google.inject.name.Named;
import io.airlift.log.Logger;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.analysis.ConfigManager;
import org.rakam.analysis.InternalConfig;
import org.rakam.analysis.JDBCPoolDataSource;
import org.rakam.collection.FieldType;
import org.rakam.plugin.user.mailbox.Message;
import org.rakam.plugin.user.mailbox.UserMailboxStorage;
import org.rakam.postgresql.report.PostgresqlQueryExecutor;
import org.rakam.util.RakamException;

import javax.inject.Inject;
import java.sql.*;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static java.lang.String.format;
import static org.rakam.util.ValidationUtil.checkProject;

public class PostgresqlUserMailboxStorage implements UserMailboxStorage {
    private final static Logger LOGGER = Logger.get(PostgresqlUserMailboxStorage.class);
    private final static String USER_NOTIFICATION_SUFFIX = "_user_mailbox";
    private final static String USER_NOTIFICATION_ALL_SUFFIX = "_user_mailbox_all_listener";
    private final PostgresqlQueryExecutor queryExecutor;
    private final JDBCPoolDataSource dataSource;
    private final LoadingCache<String, Boolean> userTypeCache;

    private AtomicLong lastMessage = new AtomicLong(Instant.now().getEpochSecond());

    @Inject
    public PostgresqlUserMailboxStorage(PostgresqlQueryExecutor queryExecutor, ConfigManager configManager, @Named("async-postgresql") JDBCPoolDataSource dataSource) {
        this.queryExecutor = queryExecutor;
        this.dataSource = dataSource;
        userTypeCache = CacheBuilder.newBuilder().build(new CacheLoader<String, Boolean>() {
            @Override
            public Boolean load(String key) throws Exception {
                return configManager.getConfig(key, InternalConfig.USER_TYPE.name(), FieldType.class) == FieldType.STRING;
            }
        });
    }

    @Override
    public Message send(String project, Object fromUser, Object toUser, Integer parentId, String message, Instant date) {
        try (Connection connection = queryExecutor.getConnection()) {
            PreparedStatement ps = connection.prepareStatement(String.format("INSERT INTO %s._user_mailbox (from_user, to_user, parentId, content, time) VALUES (?, ?, ?, ?, ?)", checkProject(project, '"')),
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
            return new Message(generatedKeys.getInt(1), fromUser, toUser, message, parentId, false, date.toEpochMilli());
        } catch (SQLException e) {
            LOGGER.error(e, "Error while saving user message");
            throw Throwables.propagate(e);
        }
    }

//    @Override
//    public void sendBatch(String project, String fromUser, List<String> toUser, Integer parentId, String message, Instant date) {
//        try (Connection connection = queryExecutor.getConnection()) {
//            PreparedStatement ps = connection.prepareStatement("INSERT INTO " + project + "._user_mailbox (from_user, to_user, parentId, content, time) VALUES (?, ?, ?, ?, ?)",
//                    Statement.RETURN_GENERATED_KEYS);
//            connection.setAutoCommit(false);
//
//            for (String user : toUser) {
//                ps.setString(1, fromUser);
//                ps.setString(2, user);
//                ps.setObject(3, parentId);
//                String escapedMessage = HtmlEscapers.htmlEscaper().escape(message);
//                ps.setString(4, escapedMessage);
//                ps.setTimestamp(5, Timestamp.from(date));
//                ps.executeUpdate();
//            }
//
//            connection.setAutoCommit(true);
//            connection.commit();
//
//        } catch (SQLException e) {
//            LOGGER.error(e, "Error while saving user message");
//            throw Throwables.propagate(e);
//        }
//    }

    @Override
    public void createProjectIfNotExists(String projectId, boolean userIdIsNumeric) {
        try (Connection connection = queryExecutor.getConnection()) {
            String tableName = format("%s._user_mailbox", projectId);
            Statement statement = connection.createStatement();
            statement.execute(format("CREATE TABLE IF NOT EXISTS %s(" +
                    "  id SERIAL," +
                    "  to_user " + (userIdIsNumeric ? "int4" : "text") + " NOT NULL," +
                    "  from_user " + (userIdIsNumeric ? "int4" : "text") + " NOT NULL," +
                    "  content TEXT NOT NULL," +
                    "  parentId INT," +
                    "  seen BOOL DEFAULT FALSE NOT NULL," +
                    "  time TIMESTAMPTZ NOT NULL," +
                    "  PRIMARY KEY (id)" +
                    "  )", tableName));

            String msg = "'msg\n" +
                    "{\"id\":' || NEW.id ||', \"to_user\": ' || NEW.to_user || ', \"from_user\": ' || NEW.from_user || ', \"content\": '||to_json(NEW.content)||', \"parent_id\": '||NEW.parentid||', \"seen\": '||NEW.seen||', \"time\": '||extract(epoch from NEW.time at time zone 'utc')*1000||'}'";
            statement.execute(format("CREATE OR REPLACE FUNCTION user_mailbox_notification()" +
                    "  RETURNS trigger AS" +
                    "  $BODY$" +
                    "    BEGIN" +
                    "        PERFORM pg_notify('%1$s_' || NEW.to_user || '" + USER_NOTIFICATION_SUFFIX + "', " + msg + ");" +
                    "        PERFORM pg_notify('%1$s_' || NEW.from_user || '" + USER_NOTIFICATION_SUFFIX + "', " + msg + ");" +
                    "        PERFORM pg_notify('%1$s" + USER_NOTIFICATION_ALL_SUFFIX + "', " + msg + ");" +
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
//        try (Connection conn = dataSource.getConnection()) {
//            final PGConnection unwrap = conn.unwrap(PGConnection.class);
//            String name = projectId + "_" + user + USER_NOTIFICATION_SUFFIX;
//
//            PGNotificationListener listener = (processId, channelName, payload) -> {
//                int idx = payload.indexOf("\n");
//                Operation op = Operation.valueOf(payload.substring(0, idx));
//                consumer.accept(new Data(op, payload.substring(idx + 1)));
//            };
//            unwrap.addNotificationListener(name, listener);
//
//            try (Statement statement = unwrap.createStatement()) {
//                statement.execute("LISTEN " + name);
//            } catch (SQLException e) {
//                throw Throwables.propagate(e);
//            }
//
//            return () -> {
//                try (Statement statement = unwrap.createStatement()) {
//                    statement.execute(format("UNLISTEN %s_%s" + USER_NOTIFICATION_SUFFIX, projectId, user));
//                } catch (SQLException e) {
//                    throw Throwables.propagate(e);
//                }
//                unwrap.removeNotificationListener(listener);
//            };
//
//        } catch (SQLException e) {
//            throw Throwables.propagate(e);
//        }
        return null;
    }

    @Override
    public MessageListener listenAllUsers(String projectId, Consumer<Data> consumer) {
//        try (Connection conn = dataSource.getConnection()) {
//            PGConnection asyncConn = conn.unwrap(PGConnection.class);
//            PGNotificationListener listener = (processId, channelName, payload) -> {
//                if (lastMessage.get() + 2 > Instant.now().getEpochSecond()) {
//                    return;
//                }
//                int idx = payload.indexOf("\n");
//                Operation op = Operation.valueOf(payload.substring(0, idx));
//                consumer.accept(new Data(op, payload.substring(idx + 1)));
//                lastMessage.set(Instant.now().getEpochSecond());
//            };
//            asyncConn.addNotificationListener(projectId + USER_NOTIFICATION_ALL_SUFFIX, listener);
//
//            try (Statement statement = asyncConn.createStatement()) {
//                statement.execute("LISTEN " + projectId + USER_NOTIFICATION_ALL_SUFFIX);
//            } catch (SQLException e) {
//                throw Throwables.propagate(e);
//            }
//
//            return () -> {
//                try (Statement statement = asyncConn.createStatement()) {
//                    statement.execute(format("UNLISTEN %s_user_mailbox_all_listener", projectId));
//                } catch (SQLException e) {
//                    throw Throwables.propagate(e);
//                }
//                asyncConn.removeNotificationListener(listener);
//            };
//        } catch (SQLException e) {
//            throw Throwables.propagate(e);
//        }
        return null;
    }

    @Override
    public List<Message> getConversation(String project, String userId, Integer parentId, int limit, long offset) {
        try (Connection connection = queryExecutor.getConnection()) {
            PreparedStatement ps;

            if (parentId == null) {
                ps = connection.prepareStatement("SELECT id, from_user, content, seen, time, to_user FROM " + project +
                        "._user_mailbox WHERE (to_user = ? OR from_user = ?) AND parentId IS NULL ORDER BY time DESC LIMIT ? OFFSET ?");
                ps.setInt(3, limit);
                ps.setLong(4, offset);
            } else {
                ps = connection.prepareStatement("SELECT id, from_user, content, seen, time, to_user FROM " + project +
                        "._user_mailbox WHERE (to_user = ? OR from_user = ?) AND (parentId = ? OR id = ?) ORDER BY time DESC LIMIT ? OFFSET ?");
                ps.setInt(3, parentId);
                ps.setInt(4, parentId);
                ps.setInt(5, limit);
                ps.setLong(6, offset);
            }

            if (userTypeCache.getUnchecked(project) == Boolean.TRUE) {
                ps.setString(1, userId.toString());
                ps.setString(2, userId.toString());
            } else {
                long x;
                try {
                    x = Long.parseLong(userId.toString());
                } catch (NumberFormatException e) {
                    throw new RakamException("User id is invalid", HttpResponseStatus.BAD_REQUEST);
                }

                ps.setLong(1, x);
                ps.setLong(2, x);
            }

            ResultSet resultSet = ps.executeQuery();
            ImmutableList.Builder<Message> builder = ImmutableList.builder();
            while (resultSet.next()) {
                builder.add(new Message(resultSet.getInt(1), resultSet.getObject(2), resultSet.getObject(6),
                        resultSet.getString(3), parentId,
                        resultSet.getBoolean(4), resultSet.getTimestamp(5).toInstant().toEpochMilli()));
            }
            return builder.build();
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void markMessagesAsRead(String project, String userId, int[] messageIds) {
        try (Connection connection = queryExecutor.getConnection()) {
            PreparedStatement ps = connection.prepareStatement("UPDATE " + project +
                    "._user_mailbox SET seen = true WHERE to_user = ? and id in ?");
            ps.setString(1, userId);
            ps.setArray(2, connection.createArrayOf("int", Arrays.stream(messageIds).mapToObj(Integer::new).toArray()));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }
}
