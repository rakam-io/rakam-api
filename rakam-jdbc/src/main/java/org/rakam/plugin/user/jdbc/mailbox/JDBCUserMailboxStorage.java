package org.rakam.plugin.user.jdbc.mailbox;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.rakam.JDBCConfig;
import org.rakam.plugin.user.mailbox.Message;
import org.rakam.plugin.user.mailbox.UserMailboxStorage;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Created by buremba <Burak Emre Kabakcı> on 30/03/15 06:22.
 */
public class JDBCUserMailboxStorage implements UserMailboxStorage {
    private final Handle dao;

    @Inject
    public JDBCUserMailboxStorage(@Named("plugin.user.mailbox.persistence.jdbc") JDBCConfig config) {
        DBI dbi = new DBI(format(config.getUrl(), config.getUsername(), config.getPassword()),
                config.getUsername(), config.getUsername());
        dao = dbi.open();
    }

    @Override
    public Message send(String project, Object userId, String message) {
        dao.createStatement(format("INSERT INTO user_mailbox_%s (message), VALUES (:message)")).bind("message", message)
                .executeAndReturnGeneratedKeys().first();
        return new Message(0, message, null, false);
    }

    @Override
    public void createProject(String projectId) {
        dao.createStatement(format("CREATE TABLE IF NOT EXISTS user_mailbox_%s (" +
                "  id SERIAL," +
                "  user TEXT NOT NULL," +
                "  message TEXT NOT NULL," +
                "  parentId INT," +
                "  seen BOOL," +
                "  time TIME," +
                "  PRIMARY KEY (id)" +
                "  )", projectId))
                .execute();
    }

    @Override
    public List<Message> getMessages(String project, Object userId, int limit, int offset) {
        return dao.createQuery(format("SELECT id, message, parentId, seen FROM user_mailbox_%s WHERE user = :user " +
                "ORDER BY time DESC LIMIT %d OFFSET %S", project, limit, offset)).map((index, r, ctx) -> {
                    return new Message(r.getInt(1), r.getString(2), r.getInt(3), r.getBoolean(4));
                }).list();
    }

    @Override
    public void markMessagesAsRead(String project, Object userId, int[] messageIds) {
        if(messageIds.length == 0) {
            return;
        }
        String sql = Arrays.stream(messageIds).mapToObj(i -> "user = " + i).collect(Collectors.joining(" OR "));

        dao.createStatement(format("UPDATE user_mailbox_%s (seen), VALUES (true) WHERE " + sql, project)).execute();
    }
}
