package org.rakam.plugin.user.mailbox;

import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;

/**
 * Created by buremba <Burak Emre Kabakcı> on 16/03/15 21:33.
 */
public interface UserMailboxStorage {
    public Message send(String project, Object fromUser, Object toUser, Integer parentId, String message, Instant date);
    public void createProject(String projectId);
    public MessageListener listen(String projectId, String user, Consumer<Message> messageConsumer);
    public MessageListener listenAllUsers(String projectId, Consumer<Message> messageConsumer);
    List<Message> getConversation(String project, Object userId, Integer parentId, int limit, int offset);
    public void markMessagesAsRead(String project, Object userId, int[] messageIds);

    public static interface MessageListener {
        public void shutdown();
    }
}
